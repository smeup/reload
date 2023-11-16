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

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue


class SQLMunicipalityPerfTest {

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
    fun read() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        for(index in 0..138){
            var result = dbFile.read();
            assertTrue{!dbFile.eof()}
            if(index == 138) {
                assertEquals("CO" , getMunicipalityProv(result.record))
            }
        }
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun deleteChain() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult2 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertEquals("ERBUSCO", getMunicipalityName(chainResult2.record))
        dbFile.delete(chainResult2.record)
        dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertTrue(dbFile.eof())
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun deleteRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        val record = dbFile.read().record
        dbFile.delete(record)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        dbFile.read()
        assertTrue(dbFile.eof())
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun chain() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult1 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO"))
        assertEquals(0, chainResult1.record.size)
        val chainResult2 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertEquals("ERBUSCO", getMunicipalityName(chainResult2.record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
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

    @Test
    fun setllReadeNoMatch() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO")))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO"))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setllReadpe() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("EDOLO", getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun setgtReade() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS")))
        assertEquals("ALBAVILLA", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun eofAfterRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE")))
        assertEquals("ZONE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        var r = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertTrue(dbFile.eof())
        r = dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertTrue(dbFile.eof())
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun write() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)

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
        dbFile.update(record)

        // Verify changes
        chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "DALMINE"))
        assertEquals("BS", getMunicipalityProv(chainResult.record))

        // Verify that old record is missing
        chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "DALMINE"))
        assertTrue(dbFile.eof())
    }

    @Test
    fun readAndChain() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        val readResult = dbFile.read()
        assertEquals("ERBUSCO", getMunicipalityName(readResult.record))

        // Chain to another record
        var chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ADRO"))
        assertEquals("ADRO", getMunicipalityName(chainResult.record))
    }

    @Test
    fun writeAndDelete() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)

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
    }

    @Test
    fun fullTest() {
        eofAfterRead()
        setgtReade()
        setllReadpe()
        setllReadeNoMatch()
        notEqual()
        equal()
        chain()
        read()
    }

    @Test
    fun usupportedUncoherentKeys() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE"))
        assertFails {dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "CO"))}
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun usupportedDifferentReadMethods() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE"))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertFails {dbFile.read()}
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun usupportedUnpositioning() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertFails {dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))}
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun usupportedReadChangeDirection() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ZONE"))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS"))
        assertFails {dbFile.readPrevious()}
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun usupportedFeatures() {
        usupportedUncoherentKeys()
        usupportedDifferentReadMethods()
        usupportedUnpositioning()
        usupportedReadChangeDirection()
    }
}

