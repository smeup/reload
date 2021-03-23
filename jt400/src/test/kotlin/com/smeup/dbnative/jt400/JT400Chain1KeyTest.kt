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

package com.smeup.dbnative.jt400

import com.smeup.dbnative.jt400.JT400DBMMAnager
import com.smeup.dbnative.jt400.utils.*
import org.junit.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JT400Chain1KeyTest {

    private lateinit var dbManager: JT400DBMMAnager

    @Before
    fun setUp() {
        println("setup")
        dbManager = dbManagerForTest()
        createAndPopulateTestTable(dbManager)
    }

    @After
    fun tearDown() {
        println("tearDown")
        destroyDatabase()
        dbManager.close()
    }

    @Test
    fun findRecordsIfChainWithExistingKey() {
        val dbFile = dbManager.openFile(TSTTAB_TABLE_NAME)
        val chainResult = dbFile.chain("XXX")
        assertEquals("XXX", chainResult.record["TSTFLDCHR"])
        assertEquals("123.45", chainResult.record["TSTFLDNBR"])
        dbManager.closeFile(TSTTAB_TABLE_NAME)
    }

    @Test
    fun doesNotFindRecordsIfChainWithNotExistingKey() {
        val dbFile = dbManager.openFile(TSTTAB_TABLE_NAME)
        assertTrue(dbFile.chain("XYZ").record.isEmpty())
        dbManager.closeFile(TSTTAB_TABLE_NAME)
    }

}

