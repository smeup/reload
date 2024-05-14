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
        var result = dbFile.readEqual()
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
        var readed = 0;
        var readResult = Result()
        while (dbFile.eof() == false) {
            readResult = dbFile.readEqual("C01")
            readed++
        }
        assertEquals(4, readed)
        assertTrue(readResult.indicatorEQ)
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
        val dbFile = SQLReadEqualTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        while (!dbFile.eof()) {
            dbFile.read()
        }
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))

        SQLReadEqualTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
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
        val dbFile = SQLReadEqualTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM"))
        assertEquals("ALBAVILLA", getMunicipalityName(result.record))
        SQLReadEqualTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun eofAfterRead() {
        val dbFile = SQLReadEqualTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE")))
        assertEquals("ZONE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        var r = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertTrue(dbFile.eof())
        assertTrue(r.indicatorEQ)
        r = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertTrue(dbFile.eof())
        SQLReadEqualTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setllReadeNoMatch() {
        val dbFile = SQLReadEqualTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO")))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO"))
        SQLReadEqualTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setGtReadsetGtReade() {
        val dbFile = SQLReadEqualTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        while(!dbFile.eof()) {
            dbFile.read()
        }
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))

        SQLReadEqualTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setLlReadEWithMoreKeys() {
        val dbFile = SQLReadEqualTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM")))
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(result.record))

        SQLReadEqualTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }
}

