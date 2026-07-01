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

import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SQLExecuteQueryTest {

    companion object {
        private lateinit var dbManager: SQLDBMManager

        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateMunicipalityTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            destroyDatabase()
        }
    }

    @Test
    fun `executeQuery returns rows matching placeholder filter`() {
        val count = dbManager.executeQuery(
            SQLQuery("SELECT * FROM \"$MUNICIPALITY_TABLE_NAME\" WHERE \"NAZ\" = ?", listOf("IT"))
        ) { rs ->
            var n = 0
            while (rs.next()) {
                assertEquals("IT", rs.getString("NAZ").trim())
                n++
            }
            n
        }
        assertTrue(count > 0)
    }

    @Test
    fun `executeQuery with no matching rows yields empty ResultSet`() {
        val hasRows = dbManager.executeQuery(
            SQLQuery("SELECT * FROM \"$MUNICIPALITY_TABLE_NAME\" WHERE \"NAZ\" = ?", listOf("XX"))
        ) { rs -> rs.next() }
        assertFalse(hasRows)
    }

    @Test
    fun `executeQuery without parameters returns all rows`() {
        val hasRows = dbManager.executeQuery(
            SQLQuery("SELECT * FROM \"$MUNICIPALITY_TABLE_NAME\"")
        ) { rs -> rs.next() }
        assertTrue(hasRows)
    }

    @Test
    fun `resources are closed after block — second call on same query succeeds`() {
        repeat(2) {
            dbManager.executeQuery(SQLQuery("SELECT * FROM \"$MUNICIPALITY_TABLE_NAME\"")) { rs ->
                assertTrue(rs.next())
            }
        }
    }
}
