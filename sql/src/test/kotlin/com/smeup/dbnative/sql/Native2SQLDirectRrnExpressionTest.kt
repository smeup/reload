/*
 * Copyright 2020 The Reload project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.smeup.dbnative.sql

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Pure SQL-text coverage for the DB2400Dialect/PostgreSQLDialect "direct expression" follow-on
 * (Native2SQLAdapter.directRrnExpr): since neither dialect can be exercised against a live
 * database in this environment (no AS400 partition, no Docker for PostgreSQL here), this asserts
 * directly on the SQL [Native2SQL] builds - no DB connection is involved in any of these methods.
 *
 * Covers both RRN directions on an unkeyed (RRN mode) file for both dialects: query-by-RRN
 * (CHAIN, SETLL/SETGT+READE paging) now filters/orders by the dialect's raw rrnSelectExpression
 * instead of wrapping FROM in a ROW_NUMBER() derived table, and the output RRN column is that same
 * expression aliased into the SELECT list - no "ROW_NUMBER()" anywhere in the generated SQL.
 */
class Native2SQLDirectRrnExpressionTest {

    private val unkeyedFields = listOf(Field("CODE"), Field("DESCR"))
    private val unkeyedMetadata = FileMetadata("UNKEYED", "UNKEYED_TABLE", unkeyedFields, emptyList())

    private val keyedFields = listOf(Field("CODE"), Field("DESCR"))
    private val keyedMetadata = FileMetadata("KEYED", "KEYED_TABLE", keyedFields, listOf("CODE"))

    private fun adapterFor(dialect: SQLDialect) =
        Native2SQL(unkeyedMetadata, dialect, rrnOrderingColumns = listOf("CODE"))

    private fun keyedAdapterFor(dialect: SQLDialect) = Native2SQL(keyedMetadata, dialect)

    @Test
    fun db2400ChainByRrnUsesNativeRrnFunctionDirectly() {
        val adapter = adapterFor(DB2400Dialect())
        adapter.setRead(ReadMethod.CHAIN, listOf("5"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("5"), params)
        assertTrue(sql.contains("RRN(\"UNKEYED_TABLE\") = ?"), "was: $sql")
        assertTrue(sql.contains("RRN(\"UNKEYED_TABLE\") AS \"RRN__\""), "was: $sql")
        assertTrue(sql.startsWith("SELECT \"CODE\", \"DESCR\", RRN(\"UNKEYED_TABLE\") AS \"RRN__\" FROM \"UNKEYED_TABLE\" WHERE"), "was: $sql")
        assertFalse(sql.contains("ROW_NUMBER"), "was: $sql")
    }

    @Test
    fun postgresChainByRrnUsesRnnIdentityColumnDirectly() {
        val adapter = adapterFor(PostgreSQLDialect())
        adapter.setRead(ReadMethod.CHAIN, listOf("5"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("5"), params)
        // CAST(? AS BIGINT), not a plain ?: PostgreSQL refuses to compare a bigint column against
        // an un-cast VARCHAR-bound parameter (confirmed against a real table - see
        // SQLDialect.rrnParameterPlaceholder's kdoc).
        assertTrue(sql.contains("\"UNKEYED_TABLE\".\"__RNN\" = CAST(? AS BIGINT)"), "was: $sql")
        assertTrue(sql.contains("\"UNKEYED_TABLE\".\"__RNN\" AS \"RRN__\""), "was: $sql")
        assertTrue(sql.startsWith("SELECT \"CODE\", \"DESCR\", \"UNKEYED_TABLE\".\"__RNN\" AS \"RRN__\" FROM \"UNKEYED_TABLE\" WHERE"), "was: $sql")
        assertFalse(sql.contains("ROW_NUMBER"), "was: $sql")
    }

    @Test
    fun db2400PositioningPagesByNativeRrnFunctionNotRowNumber() {
        val adapter = adapterFor(DB2400Dialect())
        adapter.setPositioning(PositioningMethod.SETLL, listOf("5"))
        adapter.setRead(ReadMethod.READE, listOf("5"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("5"), params)
        assertTrue(sql.contains("RRN(\"UNKEYED_TABLE\") >= ?"), "was: $sql")
        assertTrue(sql.contains("ORDER BY RRN(\"UNKEYED_TABLE\") ASC"), "was: $sql")
        assertFalse(sql.contains("ROW_NUMBER"), "was: $sql")

        // Page-resume (once a page of pageSize() rows is exhausted) also stays on the direct
        // expression, seeking strictly past the last-read row's RRN.
        val lastRow = Record(RecordField("CODE", "C"), RecordField("DESCR", "d"), RecordField("RRN__", "5"))
        val (resumeSql, resumeParams) = adapter.getResumeSqlStatement(lastRow)
        assertEquals(listOf("5"), resumeParams)
        assertTrue(resumeSql.contains("RRN(\"UNKEYED_TABLE\") > ?"), "was: $resumeSql")
        assertFalse(resumeSql.contains("ROW_NUMBER"), "was: $resumeSql")
    }

    @Test
    fun postgresPositioningPagesByRnnIdentityColumnNotRowNumber() {
        val adapter = adapterFor(PostgreSQLDialect())
        adapter.setPositioning(PositioningMethod.SETGT, listOf("5"))
        adapter.setRead(ReadMethod.READE, listOf("5"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("5"), params)
        assertTrue(sql.contains("\"UNKEYED_TABLE\".\"__RNN\" > CAST(? AS BIGINT)"), "was: $sql")
        assertTrue(sql.contains("ORDER BY \"UNKEYED_TABLE\".\"__RNN\" ASC"), "was: $sql")
        assertTrue(sql.contains("FETCH FIRST 100 ROWS ONLY"), "was: $sql")
        assertFalse(sql.contains("ROW_NUMBER"), "was: $sql")
    }

    @Test
    fun db2400KeyedReadEqualAfterSetllStillProjectsOutputRrn() {
        // The output-RRN direction (this plan's actual deliverable) must not depend on rrnMode:
        // a KEYED file, positioned by SETLL on its real key (not RRN) then READE, still gets the
        // RRN() expression in the SELECT list - hasOutputRrn()/outerColumns() aren't rrnMode-gated,
        // only the query-BY-RRN direction (directRrnExpr/checkKeys) is.
        val adapter = keyedAdapterFor(DB2400Dialect())
        adapter.setPositioning(PositioningMethod.SETLL, listOf("C"))
        adapter.setRead(ReadMethod.READE, listOf("C"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("C"), params)
        assertTrue(sql.contains("RRN(\"KEYED_TABLE\") AS \"RRN__\""), "was: $sql")
        // Positioning itself is still by the real key column, not by RRN.
        assertTrue(sql.contains("\"CODE\" >= ?"), "was: $sql")
        assertFalse(sql.contains("ROW_NUMBER"), "was: $sql")
    }

    @Test
    fun postgresKeyedChainStillProjectsOutputRrn() {
        val adapter = keyedAdapterFor(PostgreSQLDialect())
        adapter.setRead(ReadMethod.CHAIN, listOf("C"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("C"), params)
        assertTrue(sql.contains("\"KEYED_TABLE\".\"__RNN\" AS \"RRN__\""), "was: $sql")
        assertTrue(sql.contains("\"CODE\" = ?"), "was: $sql")
    }

    @Test
    fun defaultDialectStillFallsBackToRowNumberWrappedFrom() {
        // Regression guard: DefaultSQLDialect has no rrnSelectExpression, so it must keep using
        // the ROW_NUMBER()-wrapped FROM clause exactly as before this follow-on.
        val adapter = adapterFor(DefaultSQLDialect())
        adapter.setRead(ReadMethod.CHAIN, listOf("5"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("5"), params)
        assertTrue(sql.contains("ROW_NUMBER() OVER ()"), "was: $sql")
        assertTrue(sql.contains("\"RRN__\" = ?"), "was: $sql")
    }
}
