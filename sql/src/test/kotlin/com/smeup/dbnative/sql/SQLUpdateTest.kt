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

import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SQLUpdateTest {
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
    fun update() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        // Chain to record to modify
        var chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "DALMINE"))
        assertEquals("DALMINE", getMunicipalityName(chainResult.record))
        assertEquals("BG", getMunicipalityProv(chainResult.record))

        // Update
        val record = chainResult.record
        record.set("PROV", "BS")
        dbFile.update(record, )

        // Verify changes
        chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "DALMINE"))
        assertEquals("BS", getMunicipalityProv(chainResult.record))

        // Verify that old record is missing
        chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "DALMINE"))
        assertTrue(chainResult.record.size == 0)
        SQLUpdateTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

}

