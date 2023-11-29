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

import com.smeup.dbnative.sql.utils.TSTTAB_TABLE_NAME
import com.smeup.dbnative.sql.utils.createAndPopulateTstTable
import com.smeup.dbnative.sql.utils.dbManagerForTest
import com.smeup.dbnative.sql.utils.destroyDatabase
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SQLChain1KeyTest {

    companion object {

        private var dbManager: SQLDBMManager? = null
        private var libName: String? = null

        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateTstTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            destroyDatabase()
        }
    }

    @Test
    fun findRecordsIfChainWithExistingKey() {
        val dbFile = dbManager!!.openFile(TSTTAB_TABLE_NAME)
        val chainResult = dbFile.chain("XXX")
        assertEquals("XXX", chainResult.record["TSTFLDCHR"])
        assertEquals("123.45", chainResult.record["TSTFLDNBR"])
        dbManager!!.closeFile(TSTTAB_TABLE_NAME)
    }

    @Test
    fun doesNotFindRecordsIfChainWithNotExistingKey() {
        val dbFile = dbManager!!.openFile(TSTTAB_TABLE_NAME)
        assertTrue(dbFile.chain("XYZ").record.isEmpty())
        dbManager!!.closeFile(TSTTAB_TABLE_NAME)
    }

}

