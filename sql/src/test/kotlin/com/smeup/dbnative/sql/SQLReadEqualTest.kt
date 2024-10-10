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

import com.smeup.dbnative.file.Result
import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.Ignore
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
            createAndPopulateMunicipalityTable(dbManager)
            createAndPopulateEmployeeTable(dbManager)
            createAndPopulateEmployeeView(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            destroyDatabase()
        }
    }

    @Test
    fun errorIfImmediatelyReadE() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        val result = dbFile.readEqual()
        assertTrue(result.indicatorLO)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun doesNotFindRecordsIfChainAndReadENotExistingKey() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        val chainResult = dbFile.setll("XXX")
        assertEquals(0, dbFile.readEqual().record.size)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun findRecordsIfChainAndReadEExistingKey() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        val setllResult = dbFile.setll("C01")
        assertEquals("SALLY KWAN", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("DELORES QUINTANA", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("HEATHER NICHOLLS", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("KIM NATZ", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals(0, dbFile.readEqual().record.size)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun readUntilEof() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        val setllResult = dbFile.setll("C01")
        var readed = 0
        var readResult = Result()
        while (dbFile.eof() == false) {
            readResult = dbFile.readEqual("C01")
            readed++
        }
        // Check tha the last element is empty with EOF set to on
        assertEquals(5, readed)
        assertTrue(readResult.record.size == 0)
        assertTrue(readResult.indicatorEQ)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun readUntilExist() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        dbFile.setll("C01")
        var readed = 0
        var readResult: Result
        while (true) {
            readResult = dbFile.readEqual("C01")
            if (readResult.record.size == 0) break
            readed++
        }
        assertEquals(4, readed)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }



    @Test
    fun equals() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        val chainResult = dbFile.setll("C01")
        assertTrue(dbFile.equal())
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun findRecordsIfReadEWithKeyExistingKey() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        assertEquals("CHRISTINE HAAS", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals("VINCENZO LUCCHESSI", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals("SEAN O'CONNELL", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals("DIAN HEMMINGER", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals("GREG ORLANDO", getEmployeeName(dbFile.readEqual("A00").record))
        assertEquals(0, dbFile.readEqual().record.size)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun setllReadsetllReade() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        while (!dbFile.eof()) {
            dbFile.read()
        }
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))

        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun doesNotFindRecordsIfReadEWithKeyNotExistingKey() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        assertEquals(0, dbFile.readEqual("XXX").record.size)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun findRecordsIfSetllAndReadEWithKeyExistingKey() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        assertTrue(dbFile.setll("C01"))
        assertEquals("SALLY KWAN", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("DELORES QUINTANA", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("HEATHER NICHOLLS", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals("KIM NATZ", getEmployeeName(dbFile.readEqual("C01").record))
        assertEquals(0, dbFile.readEqual("C01").record.size)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun setgtReade() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM"))
        assertEquals("ALBAVILLA", getMunicipalityName(result.record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Ignore
    @Test
    fun eofAfterRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        // Point to last record for IT-LOM_BS
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE")))
        var r = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ZONE", getMunicipalityName(r.record))
        // Try to read next record for IT-LOM_BS (do not exists!)
        r = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        // The returned record has to be empty with eof indicator on
        assertTrue(r.record.size == 0)
        assertTrue(dbFile.eof())
        assertTrue(r.indicatorEQ)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setllReadeNoMatch() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO")))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO"))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setGtReadsetGtReade() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        while(!dbFile.eof()) {
            dbFile.read()
        }
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))

        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    // Skip this test because fails but it should work.
    @Ignore
    @Test
    fun setLlReadEWithMoreKeys() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))

        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    // Skip this test because fails but it should work.
    @Ignore
    @Test
    fun setLlReadEWithChangedKeys() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "CO"))
        assertEquals("ALBAVILLA", getMunicipalityName(result.record))

        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    // Skip this test because fails but it should work.
    @Ignore
    @Test
    fun setLlReadEWithVariableKeys() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        var result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))
        result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setMultipleSetllReade() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        var result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "CO")))
        result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "CO"))
        assertEquals("ALBAVILLA", getMunicipalityName(result.record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    /**
     * An incoherent call is a READE that changes the keys set by the previous
     * positioning instruction.
     */
    @Test
    fun testUncoherentReadError() {
        // Wrong mode: make a IT-LOM-BS read on a previous IT-CAL-CS pointing
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "CAL", "CS")))
        var result = dbFile.readEqual(buildMunicipalityKey("IT", "CAL", "CS"))
        assertEquals("ACQUAFORMOSA", getMunicipalityName(result.record))
        assertFailsWith<IllegalArgumentException>() {
            dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        }

        // Right mode: repoint to IT-LOM-BS before change read keys from IT-CAL-CS to
        // IT-LOM-BS
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "CAL", "CS")))
        result = dbFile.readEqual(buildMunicipalityKey("IT", "CAL", "CS"))
        assertEquals("ACQUAFORMOSA", getMunicipalityName(result.record))

        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))

        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }
}