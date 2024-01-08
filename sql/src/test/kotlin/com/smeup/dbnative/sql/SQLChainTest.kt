/*
 *  Copyright 2023 The Reload project Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package com.smeup.dbnative.sql

import com.smeup.dbnative.log.LoggingKey
import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.system.measureTimeMillis
import kotlin.test.assertEquals

class SQLChainTest {
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
    fun chainNotExisting() {
        val dbFile = SQLChainTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult1 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO"))
        assertEquals(0, chainResult1.record.size)
        SQLChainTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun chainWithReducedKeys() {
        val dbFile = SQLChainTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        var chainResult = dbFile.chain(buildCountryKey("IT", "LOM", "BS"))
        assertEquals("ACQUAFREDDA", getMunicipalityName(chainResult.record))
        chainResult = dbFile.chain(buildCountryKey("IT", "LOM", "CO"))
        assertEquals("ALBAVILLA", getMunicipalityName(chainResult.record))
        chainResult = dbFile.chain(buildRegionKey("IT", "LOM"))
        assertEquals("ADRARA SAN MARTINO", getMunicipalityName(chainResult.record))
        chainResult = dbFile.chain(buildNationKey("IT"))
        assertEquals("ACCIANO", getMunicipalityName(chainResult.record))
        SQLChainTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun chainExisting1() {
        val dbFile = SQLChainTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult2 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertEquals("ERBUSCO", getMunicipalityName(chainResult2.record))
        SQLChainTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun chainExisting2() {
        val dbFile = SQLChainTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult2 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ROVATO"))
        assertEquals("ROVATO", getMunicipalityName(chainResult2.record))
        SQLChainTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun multipleChainExisting() {
        val dbFile = SQLChainTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertEquals("ERBUSCO", getMunicipalityName(chainResult.record))
        val chainResult2 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ROVATO"))
        assertEquals("ROVATO", getMunicipalityName(chainResult2.record))
        SQLChainTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun readAndChain() {
        val dbFile = SQLChainTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        val readResult = dbFile.read()
        assertEquals("ERBUSCO", getMunicipalityName(readResult.record))

        // Chain to another record
        var chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ADRO"))
        assertEquals("ADRO", getMunicipalityName(chainResult.record))
        SQLChainTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun chainLoop() {
        val dbFile = SQLChainTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val n = 50
        measureTimeMillis {
            for (i in 0..n) {
                dbFile.chain(buildRegionKey("IT", "LOM"))
                dbFile.chain(buildRegionKey("IT", "LAZ"))
            }
        }.apply {
            println("Time doing $n chain: $this")
        }
        /*dbFile.close()
        SQLChainTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)*/
    }

    @Test
    fun chainRedisTest() {
        val dbFile = SQLChainTest.dbManager.openFile("BRCOMM0L")
        val keys = mutableListOf<String>()
        for (i in 0..50) {
            keys.add("IOT.001   ")
            dbFile.chain(keys)
            keys.remove("IOT.001   ")
            keys.add("R2-20-0281")
            dbFile.chain(keys)
            keys.remove("R2-20-0281")
        }


    }
}

