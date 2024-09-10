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
import org.junit.*
import kotlin.test.assertEquals

class JDBCUtilsTest {


    companion object {

        private lateinit var dbManager: SQLDBMManager

        lateinit var primaryKeys : List<Any>
        lateinit var orderingFields : List<Any>

        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            dbManager.connection.use {
                it.createStatement()
                    .execute("CREATE TABLE \"TSTTAB00\" (TSTFLDCHR CHAR (5) NOT NULL, TSTFLDNBR DECIMAL (7, 2) NOT NULL, TSTFLDNB2 DECIMAL (2, 0) NOT NULL, PRIMARY KEY(TSTFLDCHR, TSTFLDNBR))")
                it.createStatement()
                    .execute("CREATE VIEW \"TSTVIEW\" AS SELECT * FROM \"TSTTAB00\" ORDER BY TSTFLDNB2, TSTFLDNBR")
                it.createStatement()
                    .execute("CREATE INDEX \"TSTVIEW$CONVENTIONAL_INDEX_SUFFIX\" ON \"TSTTAB00\" (TSTFLDNB2, TSTFLDNBR)")

                primaryKeys = it.primaryKeys("\"TSTTAB00\"")
                orderingFields = it.orderingFields("\"TSTVIEW\"")
            }
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
        }
    }

    @Test
    fun primaryKeysTest() {
        if( Companion.primaryKeys.isNotEmpty()) { assertEquals(listOf("TSTFLDCHR", "TSTFLDNBR"), primaryKeys) }

    }

    @Test
    fun orderingFieldsTest() {
        if(orderingFields.isNotEmpty()){ assertEquals(listOf("TSTFLDNB2", "TSTFLDNBR"), orderingFields) }

    }

}