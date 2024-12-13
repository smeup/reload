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
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SQLReadPreviousTest {

    companion object {

        private lateinit var dbManager: SQLDBMManager
        
        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateMunicipalityTable(dbManager)
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

    @Test
    fun setllReadpe() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("EDOLO", getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setgtReadpe() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ERBUSCO", getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    /**
     * An incoherent call is a REAPE that changes the keys set by the previous
     * positioning instruction.
     */
    @Test
    fun testUncoherentReadError() {
        // Wrong mode: make a IT-CAL-CS readpe on a previous IT-LOM-BS pointing
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS")))
        var result = dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ZONE", getMunicipalityName(result.record))
        assertFailsWith<IllegalArgumentException>() {
            dbFile.readPreviousEqual(buildMunicipalityKey("IT", "CAL", "CS"))
        }

        // Right mode: repoint to IT-CAL-CS before change readpe keys from IT-LOM-BS to
        // IT-CAL-CS
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS")))
        result = dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ZONE", getMunicipalityName(result.record))

        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "CAL", "CS")))
        result = dbFile.readPreviousEqual(buildMunicipalityKey("IT", "CAL", "CS"))
        assertEquals("ZUMPANO", getMunicipalityName(result.record))

        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

}
