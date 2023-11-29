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

import com.smeup.dbnative.sql.utils.TST2TAB_TABLE_NAME
import com.smeup.dbnative.sql.utils.createAndPopulateTst2Table
import com.smeup.dbnative.sql.utils.dbManagerForTest
import com.smeup.dbnative.sql.utils.destroyDatabase
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SQLChain2KeysTest {

    companion object {

        private lateinit var dbManager: SQLDBMManager


        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateTst2Table(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            destroyDatabase()
        }
    }

    @Test
    fun findRecordsIfChainWithExistingKey() {
        val dbFile = dbManager.openFile(TST2TAB_TABLE_NAME)
        val key2 = listOf(
            "ABC",
            "12.00"
        )
        val chainResult = dbFile.chain(key2)
        assertEquals("ABC", chainResult.record["TSTFLDCHR"])
        assertEquals("12.00", chainResult.record["TSTFLDNBR"])
        assertEquals("ABC12", chainResult.record["DESTST"])
        dbManager.closeFile(TST2TAB_TABLE_NAME)
    }

    @Test
    fun doesNotFindRecordsIfChainWithNotExistingKey() {
        val dbFile = dbManager.openFile(TST2TAB_TABLE_NAME)
        val key2 = listOf(
             "ZZZ",
             "12"
        )
        assertTrue(dbFile.chain(key2).record.isEmpty())
        dbManager.closeFile(TST2TAB_TABLE_NAME)
    }


}

