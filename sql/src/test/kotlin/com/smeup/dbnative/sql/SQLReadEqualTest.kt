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

import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SQLReadEqualTest {

    companion object {

        private lateinit var dbManager: SQLDBMManager
        
        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateEmployeeTable(dbManager)
            createAndPopulateXemp2View(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            destroyDatabase()
        }
    }

    @Test
    fun throwsExceptionIfImmediatelyReadE() {
        val dbFile = dbManager.openFile(XEMP2_VIEW_NAME)
        assertFailsWith(IllegalArgumentException::class) {
            dbFile.readEqual()
        }
        dbManager.closeFile(XEMP2_VIEW_NAME)
    }

    @Test
    fun doesNotFindRecordsIfChainAndReadENotExistingKey() {
        val dbFile = dbManager.openFile(XEMP2_VIEW_NAME)
        val chainResult = dbFile.chain(listOf(RecordField("WORKDEPT", "XXX")))
        assertEquals(0, chainResult.record.size)
        assertEquals(0, dbFile.readEqual().record.size)
        dbManager.closeFile(XEMP2_VIEW_NAME)
    }

    @Test
    fun findRecordsIfChainAndReadEExistingKey() {
        val dbFile = dbManager.openFile(XEMP2_VIEW_NAME)
        val chainResult = dbFile.chain(listOf(RecordField("WORKDEPT", "C01")))
        assertEquals("SALLY KWAN", getEmployeeName(chainResult.record))
        assertEquals("DELORES QUINTANA", getEmployeeName(dbFile.readEqual().record))
        assertEquals("HEATHER NICHOLLS", getEmployeeName(dbFile.readEqual().record))
        assertEquals("KIM NATZ", getEmployeeName(dbFile.readEqual().record))
        assertEquals(0, dbFile.readEqual().record.size)
        dbManager.closeFile(XEMP2_VIEW_NAME)
    }

    @Test

    fun findRecordsIfReadEWithKeyExistingKey() {
        val dbFile = dbManager.openFile(XEMP2_VIEW_NAME)
        assertEquals("CHRISTINE HAAS", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals("VINCENZO LUCCHESSI", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals("SEAN O'CONNELL", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals("DIAN HEMMINGER", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals("GREG ORLANDO", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals(0, dbFile.readEqual().record.size)
        dbManager.closeFile(XEMP2_VIEW_NAME)
    }

    @Test
    fun doesNotFindRecordsIfReadEWithKeyNotExistingKey() {
        val dbFile = dbManager.openFile(XEMP2_VIEW_NAME)
        assertEquals(0, dbFile.readEqual("XXX").record.size)
        dbManager.closeFile(XEMP2_VIEW_NAME)
    }

    @Test
    fun findRecordsIfSetllAndReadEWithKeyExistingKey() {
        val dbFile = dbManager.openFile(XEMP2_VIEW_NAME)
        assertTrue(dbFile.setll("C01"))
        assertEquals("SALLY KWAN", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("DELORES QUINTANA", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("HEATHER NICHOLLS", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("KIM NATZ", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals(0, dbFile.readEqual("C01").record.size)
        dbManager.closeFile(XEMP2_VIEW_NAME)
    }

}

