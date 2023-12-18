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
import kotlin.test.assertTrue

class SQLReadTest {

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
    fun readUntilEof() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        var readed = 0;
        while (dbFile.eof() == false) {
            var readResult = dbFile.read()
            readed++
            System.out.println("Lettura $readed: " + getEmployeeName(readResult.record))
        }
        assertEquals(42, readed)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun positioningAndReadUntilEof() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        var readed = 0;
        val setllResult = dbFile.setll( "C01")
        while (dbFile.eof() == false) {
            var readResult = dbFile.read()
            readed++
            System.out.println("Lettura $readed: " + getEmployeeName(readResult.record))
        }
        assertEquals(36, readed)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun positioningBlankAndReadUntilEof() {
        val dbFile = dbManager.openFile(EMPLOYEE_VIEW_NAME)
        var readed = 0;
        val setllResult = dbFile.setll( "")
        while (dbFile.eof() == false) {
            var readResult = dbFile.read()
            readed++
            System.out.println("Lettura $readed: " + getEmployeeName(readResult.record))
        }
        assertEquals(42, readed)
        dbManager.closeFile(EMPLOYEE_VIEW_NAME)
    }

    @Test
    fun multipleRead() {
        val dbFile = SQLReadTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        for(index in 0..138){
            var result = dbFile.read();
            assertTrue{!dbFile.eof()}
            if(index == 138) {
                assertEquals("CO" , getMunicipalityProv(result.record))
            }
        }
        SQLReadTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun positioningWithLessKeysAndReadUntilEof() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        var readed = 0;
        val setllResult = dbFile.setll( buildCountryKey("IT", "LOM", "BS"))
        while (dbFile.eof() == false) {
            var readResult = dbFile.read()
            readed++
            System.out.println("Lettura $readed: " + getMunicipalityName(readResult.record))
        }
        assertEquals(1001, readed)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun positioningWithHalfKeysAndReadUntilEof() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        var readed = 0;
        val setllResult = dbFile.setll( buildRegionKey("IT", "LOM"))
        while (dbFile.eof() == false) {
            var readResult = dbFile.read()
            readed++
            System.out.println("Lettura $readed: " + getMunicipalityName(readResult.record))
        }
        assertEquals(1244, readed)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }
}

