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
import kotlin.test.Test
import kotlin.test.assertTrue

class SQLEqualsTest {

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
            destroyView()
            destroyIndex()
        }
    }

    @Test
    fun equal() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertTrue(dbFile.equal())
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun notEqual() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO"))
        assertTrue(!dbFile.equal())
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }
}

