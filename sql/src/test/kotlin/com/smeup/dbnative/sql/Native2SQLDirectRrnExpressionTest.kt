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
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Pure SQL-text coverage for the "direct RRN expression" every dialect uses
 * (Native2SQLAdapter.directRrnExpr): DB2400Dialect's native `RRN()`, and the `__RNN` identity
 * column for PostgreSQLDialect and DefaultSQLDialect. Since DB2 and PostgreSQL can't be exercised
 * against a live database in this environment (no AS400 partition, no Docker for PostgreSQL
 * here), this asserts directly on the SQL [Native2SQL] builds - no DB connection is involved in
 * any of these methods.
 *
 * Covers both RRN directions: query-by-RRN on an unkeyed (RRN mode) file (CHAIN, SETLL/SETGT+READE
 * paging) filters/orders by the dialect's raw rrnSelectExpression, and the output RRN column is
 * that same expression aliased into the SELECT list, for keyed files too - no "ROW_NUMBER()"
 * anywhere in the generated SQL.
 */
class Native2SQLDirectRrnExpressionTest {

    private val unkeyedFields = listOf(Field("CODE"), Field("DESCR"))
    private val unkeyedMetadata = FileMetadata("UNKEYED", "UNKEYED_TABLE", unkeyedFields, emptyList())

    private val keyedFields = listOf(Field("CODE"), Field("DESCR"))
    private val keyedMetadata = FileMetadata("KEYED", "KEYED_TABLE", keyedFields, listOf("CODE"))

    private fun adapterFor(dialect: SQLDialect) = Native2SQL(unkeyedMetadata, dialect)

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
        // RRN() expression in the SELECT list - outerColumns() isn't rrnMode-gated, only the
        // query-BY-RRN direction (directRrnExpr/checkKeys) is.
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
    fun defaultDialectChainByRrnUsesRnnIdentityColumnDirectly() {
        // DefaultSQLDialect (HSQLDB, H2, MySQL, ...) follows the same __RNN convention as
        // PostgreSQL - no ROW_NUMBER() fallback anymore. The placeholder stays a plain ?: only
        // PostgreSQL needs a cast to compare its bigint column against a VARCHAR-bound parameter.
        val adapter = adapterFor(DefaultSQLDialect())
        adapter.setRead(ReadMethod.CHAIN, listOf("5"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("5"), params)
        assertTrue(sql.contains("\"UNKEYED_TABLE\".\"__RNN\" = ?"), "was: $sql")
        assertTrue(sql.startsWith("SELECT \"CODE\", \"DESCR\", \"UNKEYED_TABLE\".\"__RNN\" AS \"RRN__\" FROM \"UNKEYED_TABLE\" WHERE"), "was: $sql")
        assertFalse(sql.contains("ROW_NUMBER"), "was: $sql")
    }

    @Test
    fun defaultDialectPositioningPagesByRnnIdentityColumn() {
        val adapter = adapterFor(DefaultSQLDialect())
        adapter.setPositioning(PositioningMethod.SETGT, listOf("5"))
        adapter.setRead(ReadMethod.READE, listOf("5"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("5"), params)
        assertTrue(sql.contains("\"UNKEYED_TABLE\".\"__RNN\" > ?"), "was: $sql")
        assertTrue(sql.contains("ORDER BY \"UNKEYED_TABLE\".\"__RNN\" ASC"), "was: $sql")
        assertFalse(sql.contains("ROW_NUMBER"), "was: $sql")
    }

    @Test
    fun defaultDialectKeyedChainProjectsOutputRrn() {
        // The point of the unified convention: a keyed file on the Default dialect now gets a real
        // output RRN too (previously always null), with FROM left untouched so the ResultSet stays
        // updatable.
        val adapter = keyedAdapterFor(DefaultSQLDialect())
        adapter.setRead(ReadMethod.CHAIN, listOf("C"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("C"), params)
        assertTrue(sql.startsWith("SELECT \"CODE\", \"DESCR\", \"KEYED_TABLE\".\"__RNN\" AS \"RRN__\" FROM \"KEYED_TABLE\" WHERE"), "was: $sql")
        assertTrue(sql.contains("\"CODE\" = ?"), "was: $sql")
        assertFalse(sql.contains("ROW_NUMBER"), "was: $sql")
    }

    // --- Fallback when the table has no __RNN column (hasRrnColumn = false) ---

    @Test
    fun keyedQueryOmitsRrnProjectionWhenColumnMissing() {
        // A keyed file never addresses by RRN, so a missing __RNN only affects the opportunistic
        // output projection: it's simply left out, and the query still runs fine.
        val adapter = Native2SQL(keyedMetadata, DefaultSQLDialect(), hasRrnColumn = false)
        adapter.setRead(ReadMethod.CHAIN, listOf("C"))
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(listOf("C"), params)
        assertEquals("SELECT \"CODE\", \"DESCR\" FROM \"KEYED_TABLE\" WHERE \"CODE\" = ?", sql)
        assertFalse(sql.contains("__RNN"), "was: $sql")
        assertFalse(sql.contains("RRN__"), "was: $sql")
    }

    @Test
    fun unkeyedPlainReadDoesNotFailWhenColumnMissing() {
        // A plain unkeyed READ (no positioning, no RRN value supplied) never touches the RRN
        // column at all - it must keep working even though the table has no __RNN.
        val adapter = Native2SQL(unkeyedMetadata, DefaultSQLDialect(), hasRrnColumn = false)
        adapter.setRead(ReadMethod.READ)
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(emptyList(), params)
        assertEquals("SELECT \"CODE\", \"DESCR\" FROM \"UNKEYED_TABLE\"", sql)
    }

    @Test
    fun unkeyedPlainReadOrdersByRrnWhenColumnPresent() {
        // A plain unkeyed READ (arrival sequence, no prior SETLL/SETGT) must still come back in
        // insertion order, same guarantee the old ROW_NUMBER()-numbered derived table gave every
        // RRN-mode query (positioned or not) - see getReadCoherentSql. Without an explicit ORDER
        // BY here, the row order of a bare "SELECT ... FROM table" is left entirely to the
        // engine's scan strategy, which PostgreSQL in particular doesn't keep stable across
        // vacuums/updates/parallel scans.
        val adapter = adapterFor(PostgreSQLDialect())
        adapter.setRead(ReadMethod.READ)
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(emptyList(), params)
        assertEquals(
            "SELECT \"CODE\", \"DESCR\", \"UNKEYED_TABLE\".\"__RNN\" AS \"RRN__\" FROM \"UNKEYED_TABLE\" ORDER BY \"UNKEYED_TABLE\".\"__RNN\" ASC",
            sql
        )
    }

    @Test
    fun keyedPlainReadStaysUnorderedRegardlessOfRrnColumn() {
        // A keyed file's plain READ never had an ordering guarantee, before or after this
        // feature - only RRN mode (unkeyed) gets the ORDER BY, since only RRN mode has a
        // well-defined "arrival sequence" to preserve.
        val adapter = keyedAdapterFor(PostgreSQLDialect())
        adapter.setRead(ReadMethod.READ)
        val (sql, params) = adapter.getSQLStatement()

        assertEquals(emptyList(), params)
        assertFalse(sql.contains("ORDER BY"), "was: $sql")
    }

    @Test
    fun unkeyedChainByRrnFailsFastWithClearMessageWhenColumnMissing() {
        // Genuinely addressing an unkeyed file BY RRN with no __RNN column has no addressing
        // mechanism (the old ROW_NUMBER() fallback is gone) - this must still fail, but fast and
        // clearly, before any SQL is built, rather than a raw driver "column ... does not exist".
        val adapter = Native2SQL(unkeyedMetadata, DefaultSQLDialect(), hasRrnColumn = false)
        val ex = assertFailsWith<IllegalArgumentException> {
            adapter.setRead(ReadMethod.CHAIN, listOf("5"))
        }
        assertTrue(ex.message!!.contains("__RNN"), "was: ${ex.message}")
    }

    @Test
    fun unkeyedSetllByRrnFailsFastWithClearMessageWhenColumnMissing() {
        val adapter = Native2SQL(unkeyedMetadata, DefaultSQLDialect(), hasRrnColumn = false)
        assertFailsWith<IllegalArgumentException> {
            adapter.setPositioning(PositioningMethod.SETLL, listOf("5"))
        }
    }
}
