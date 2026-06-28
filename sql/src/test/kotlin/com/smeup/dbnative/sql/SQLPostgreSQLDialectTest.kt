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

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.testcontainers.containers.PostgreSQLContainer
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class SQLPostgreSQLDialectTest {

    companion object {
        private val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:15-alpine")
        private lateinit var dbManager: SQLDBMManager

        @BeforeClass
        @JvmStatic
        fun setUp() {
            postgres.start()
            dbManager = SQLDBMManager(
                ConnectionConfig(
                    fileName = "*",
                    url = postgres.jdbcUrl,
                    user = postgres.username,
                    password = postgres.password,
                    driver = "org.postgresql.Driver"
                )
            )
            createAndPopulateMunicipalityTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dbManager.connection.close()
            postgres.stop()
        }
    }

    @Test
    fun dialectIsPostgreSQL() {
        assertIs<PostgreSQLDialect>(dbManager.dialect)
    }

    @Test
    fun t01_setllWithAllKeysAndRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ERBUSCO", getMunicipalityName(dbFile.read().record))
        assertEquals("ESINE", getMunicipalityName(dbFile.read().record))
        assertEquals("FIESSE", getMunicipalityName(dbFile.read().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t02_setgtWithAllKeysAndRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ESINE", getMunicipalityName(dbFile.read().record))
        assertEquals("FIESSE", getMunicipalityName(dbFile.read().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t03_setllWithAllKeysAndReadEqual3Keys() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSC")))
        assertEquals("ERBUSCO", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals("ESINE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals("FIESSE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t04_setllWithAllKeysAndReadPrevious() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("EDOLO", getMunicipalityName(dbFile.readPrevious().record))
        assertEquals("DESENZANO DEL GARDA", getMunicipalityName(dbFile.readPrevious().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t05_setgtWithAllKeysAndReadPreviousEqual() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ERBUSCO", getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals("EDOLO", getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t06_setgtCrossRegionBoundaryAndRead() {
        // ZONE is the last city in Brescia; next record must be from Como (first after BS in LOM)
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ZONE")))
        assertEquals("ALBAVILLA", getMunicipalityName(dbFile.read().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t07_setllWith3KeysAndReadEqual3Keys() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val key3 = buildMunicipalityKey("IT", "BAS", "MT")
        assertTrue(dbFile.setll(key3))
        assertEquals("ACCETTURA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("ALIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("BERNALDA", getMunicipalityName(dbFile.readEqual(key3).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t08_chainWithAllKeys() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val result = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertEquals("ERBUSCO", getMunicipalityName(result.record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }
}
