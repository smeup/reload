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
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SQLUnsupportedFeaturesTest {

    companion object {

        private lateinit var dbManager: SQLDBMManager
        
        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateMunicipalityTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            destroyDatabase()
        }
    }

    @Test
    fun usupportedUncoherentKeys() {
        val dbFile = SQLUnsupportedFeaturesTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE"))
        assertFails {dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "CO"))}
        SQLUnsupportedFeaturesTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun usupportedDifferentReadMethods() {
        val dbFile = SQLUnsupportedFeaturesTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE"))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertFails {dbFile.read()}
        SQLUnsupportedFeaturesTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun usupportedUnpositioning() {
        val dbFile = SQLUnsupportedFeaturesTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val result = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertTrue { result.indicatorLO }
        SQLUnsupportedFeaturesTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun usupportedReadChangeDirection() {
        val dbFile = SQLUnsupportedFeaturesTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE"))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertFails {dbFile.readPrevious()}
        SQLUnsupportedFeaturesTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }


    @Test
    fun unsupportedReadDeleteRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BG", "COVO")))
        val record = dbFile.read().record
        try { dbFile.delete(record) } catch (e: Exception) {
            println(e)
        }
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BG", "COVO"))
        val record2 = dbFile.read().record
        println(record)
        println(record2)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }
}

