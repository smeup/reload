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

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SQLWriteDeleteTest {
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
    fun chainDeleteChain() {
        val dbFile = SQLWriteDeleteTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult2 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "CREDARO"))
        assertEquals("CREDARO", getMunicipalityName(chainResult2.record))
        dbFile.delete(chainResult2.record)
        dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "CREDARO"))
        assertTrue(dbFile.eof())
        SQLWriteDeleteTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun readDeleteRead() {
        val dbFile = SQLWriteDeleteTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BG", "COVO")))
        val record = dbFile.read().record
        dbFile.delete(record)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BG", "COVO")))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BG", "COVO"))
        assertTrue(dbFile.eof())
        SQLWriteDeleteTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun writeDelete() {
        val dbFile = SQLWriteDeleteTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        // Write new record
        val recordFields = emptyList<RecordField>().toMutableList()
        recordFields.add(RecordField("NAZ", "IT"))
        recordFields.add(RecordField("REG", "LOM"))
        recordFields.add(RecordField("PROV", "BG"))
        recordFields.add(RecordField("CITTA", "PAPEROPOLI"))
        recordFields.add(RecordField("PREF", "4321"))
        recordFields.add(RecordField("COMUNE", "A99"))
        recordFields.add(RecordField("ISTAT", "999999"))
        dbFile.write(Record(*recordFields.toTypedArray()));

        // Read new record
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BG", "PAPEROPOLI"))
        val readResult = dbFile.read()
        assertEquals("PAPEROPOLI", getMunicipalityName(readResult.record))

        // Delete added record
        var chainResult= dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "PAPEROPOLI"))
        dbFile.delete(chainResult.record)
        chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "PAPEROPOLI"))
        assertTrue(dbFile.eof())
        SQLWriteDeleteTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    fun writeChain() {
        val dbFile = SQLWriteDeleteTest.dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        // Write new record
        val recordFields = emptyList<RecordField>().toMutableList()
        recordFields.add(RecordField("NAZ", "IT"))
        recordFields.add(RecordField("REG", "LOM"))
        recordFields.add(RecordField("PROV", "BG"))
        recordFields.add(RecordField("CITTA", "TOPOLINIA"))
        recordFields.add(RecordField("PREF", "1234"))
        recordFields.add(RecordField("COMUNE", "A99"))
        recordFields.add(RecordField("ISTAT", "999999"))
        dbFile.write(Record(*recordFields.toTypedArray()));

        // Read new record for control
        var chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "TOPOLINIA"))
        assertEquals("TOPOLINIA", getMunicipalityName(chainResult.record))
        SQLWriteDeleteTest.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }


}

