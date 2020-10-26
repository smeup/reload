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

package com.smeup.reload.sql

import com.smeup.reload.sql.utils.dbManagerForTest
import com.smeup.reload.sql.utils.destroyDatabase
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class JDBCUtilsTest {

    private lateinit var dbManager: SQLDBMManager

    @Before
    fun setUpEach() {
        dbManager = dbManagerForTest()
    }

    @Test
    fun primaryKeysTest() {
        dbManager.connection.use {
            it.createStatement()
                .execute("CREATE TABLE TSTTAB00 (TSTFLDCHR CHAR (5) NOT NULL, TSTFLDNBR DECIMAL (7, 2) NOT NULL, TSTFLDNB2 DECIMAL (2, 0) NOT NULL, PRIMARY KEY(TSTFLDCHR, TSTFLDNBR))")
            assertEquals(listOf("TSTFLDCHR", "TSTFLDNBR"), it.primaryKeys("TSTTAB00"))
        }
    }

    @Test
    fun orderingFieldsTest() {
        dbManager.connection.use {
            it.createStatement()
                .execute("CREATE TABLE TSTTAB00 (TSTFLDCHR CHAR (5) NOT NULL, TSTFLDNBR DECIMAL (7, 2) NOT NULL, TSTFLDNB2 DECIMAL (2, 0) NOT NULL, PRIMARY KEY(TSTFLDCHR, TSTFLDNBR))")
            it.createStatement().execute("CREATE VIEW TSTVIEW AS SELECT * FROM TSTTAB00 ORDER BY TSTFLDNB2, TSTFLDNBR")
            it.createStatement()
                .execute("CREATE INDEX TSTVIEW$CONVENTIONAL_INDEX_SUFFIX ON TSTTAB00 (TSTFLDNB2, TSTFLDNBR)")
            assertEquals(listOf("TSTFLDNB2", "TSTFLDNBR"), it.orderingFields("TSTVIEW"))
        }
    }

    @After
    fun tearDownEach() {
        destroyDatabase()
    }
}