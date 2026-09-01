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

import com.smeup.dbnative.sql.utils.dbManagerForTest
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class SQLExecuteUpdateTest {

    private lateinit var dbManager: SQLDBMManager

    @Before
    fun setUp() {
        dbManager = dbManagerForTest()
        dbManager.connection.createStatement().use {
            it.execute("CREATE TABLE \"TSTUPD00\" (ID INT NOT NULL, NAME VARCHAR (20), PRIMARY KEY(ID))")
        }
    }

    @After
    fun tearDown() {
        dbManager.connection.createStatement().use {
            it.execute("DROP TABLE \"TSTUPD00\"")
        }
        dbManager.close()
    }

    @Test
    fun insertReturnsAffectedRows() {
        val affected = dbManager.executeUpdate(
            SQLQuery("INSERT INTO \"TSTUPD00\" (ID, NAME) VALUES (?, ?)", listOf(1, "foo"))
        )
        assertEquals(1, affected)
    }

    @Test
    fun updateReturnsAffectedRows() {
        dbManager.executeUpdate(SQLQuery("INSERT INTO \"TSTUPD00\" (ID, NAME) VALUES (?, ?)", listOf(1, "foo")))
        dbManager.executeUpdate(SQLQuery("INSERT INTO \"TSTUPD00\" (ID, NAME) VALUES (?, ?)", listOf(2, "foo")))
        dbManager.executeUpdate(SQLQuery("INSERT INTO \"TSTUPD00\" (ID, NAME) VALUES (?, ?)", listOf(3, "bar")))

        val affected = dbManager.executeUpdate(
            SQLQuery("UPDATE \"TSTUPD00\" SET NAME = ? WHERE NAME = ?", listOf("baz", "foo"))
        )
        assertEquals(2, affected)
    }

    @Test
    fun deleteReturnsAffectedRows() {
        dbManager.executeUpdate(SQLQuery("INSERT INTO \"TSTUPD00\" (ID, NAME) VALUES (?, ?)", listOf(1, "foo")))
        dbManager.executeUpdate(SQLQuery("INSERT INTO \"TSTUPD00\" (ID, NAME) VALUES (?, ?)", listOf(2, "bar")))

        val affected = dbManager.executeUpdate(
            SQLQuery("DELETE FROM \"TSTUPD00\" WHERE ID = ?", listOf(1))
        )
        assertEquals(1, affected)
    }
}
