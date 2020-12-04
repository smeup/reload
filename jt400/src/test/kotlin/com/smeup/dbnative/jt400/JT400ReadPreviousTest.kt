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

import com.smeup.dbnative.jt400.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JT400ReadPreviousTest {

    companion object {

        private lateinit var dbManager: JT400DBMMAnager
        
        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateEmployeeTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            destroyDatabase()
        }
    }

    @Test
    fun findRecordsIfSetllFromLastRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        assertTrue(dbFile.setll("200340"))
        assertEquals("HELENA WONG", getEmployeeName(dbFile.readPrevious().record))
        assertEquals("MICHELLE SPRINGER", getEmployeeName(dbFile.readPrevious().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun findRecordsIfSetGtFromLastRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        assertTrue(dbFile.setgt("200340"))
        assertEquals("ROY ALONZO", getEmployeeName(dbFile.readPrevious().record))
        assertEquals("HELENA WONG", getEmployeeName(dbFile.readPrevious().record))
        assertEquals("MICHELLE SPRINGER", getEmployeeName(dbFile.readPrevious().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

}
