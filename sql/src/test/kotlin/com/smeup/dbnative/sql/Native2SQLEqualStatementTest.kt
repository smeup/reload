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

import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The SQL behind `%EQUAL` (SETLL then equal()) only needs to know whether one row has the key, so
 * it must be capped to one row where the dialect supports `FETCH FIRST`: an uncapped statement
 * makes some drivers (PostgreSQL) ship every row sharing the key.
 */
class Native2SQLEqualStatementTest {

    private val metadata = FileMetadata("KEYED", "KEYED_TABLE", listOf(Field("CODE"), Field("DESCR")), listOf("CODE"))

    private fun equalSql(dialect: SQLDialect): Pair<String, List<String>> {
        val adapter = Native2SQL(metadata, dialect)
        adapter.setPositioning(PositioningMethod.SETLL, listOf("A"))
        return adapter.getReadSqlStatement()
    }

    @Test
    fun postgresEqualIsCappedToOneRow() {
        val (sql, params) = equalSql(PostgreSQLDialect())
        assertEquals(listOf("A"), params)
        assertTrue(sql.contains("WHERE \"CODE\" = ?"), sql)
        assertTrue(sql.endsWith(" FETCH FIRST 1 ROWS ONLY"), sql)
    }

    @Test
    fun defaultDialectEqualIsCappedToOneRow() {
        val (sql, _) = equalSql(DefaultSQLDialect())
        assertTrue(sql.endsWith(" FETCH FIRST 1 ROWS ONLY"), sql)
    }

    @Test
    fun dialectWithoutFetchFirstKeepsTheUncappedStatement() {
        // DB2400Dialect defaults to no paging (pageSize null): FETCH FIRST is not assumed there.
        val (sql, _) = equalSql(DB2400Dialect())
        assertFalse(sql.contains("FETCH FIRST"), sql)
    }

    @Test
    fun pagingDisabledKeepsTheUncappedStatement() {
        val (sql, _) = equalSql(PostgreSQLDialect(pageSize = 0))
        assertFalse(sql.contains("FETCH FIRST"), sql)
    }
}
