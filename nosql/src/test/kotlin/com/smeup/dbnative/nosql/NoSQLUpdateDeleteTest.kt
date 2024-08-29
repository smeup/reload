package com.smeup.dbnative.nosql

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.nosql.utils.*
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class NoSQLUpdateDeleteTest {

    private lateinit var dbManager: NoSQLDBMManager

    @Before
    fun initEnv() {
        dbManager = dbManagerForTest()
        createAndPopulateMunicipalityTable(dbManager)
    }

    @After
    fun destroyEnv() {

    }

    private fun buildMunicipalityKey(vararg values: String): List<String> {
        val keyValues = mutableListOf<String>()
        val keys = arrayOf("£NAZ", "§REG", "PROV", "CITTA")
        for ((index, value) in values.withIndex()) {
            if (keys.size> index) {
                keyValues.add(value)
            }
        }
        return keyValues
    }

    @Test
    fun chainAfterDelete() {
        val dbFile = this.dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        // Delete the record
        var chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ADRO"))
        if (chainResult.record.isNotEmpty()) {
            dbFile.delete(chainResult.record)
        }

        // Verify the record is not found
        chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ADRO"))
        assertTrue(chainResult.record.isEmpty())

        this.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun updateNonExistingRecord() {
        val dbFile = this.dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        // Attempt to update a non-existing record
        val nonExistingKey = buildMunicipalityKey("IT", "LOM", "BG", "NON_EXISTING")
        val chainResult = dbFile.chain(nonExistingKey)
        assertTrue(chainResult.record.isEmpty())

        // Try updating
        val record = chainResult.record
        if (record.isNotEmpty()) {
            record.set("PROV", "BS")
            dbFile.update(record)
        }

        // Verify nothing has been updated
        val result = dbFile.chain(nonExistingKey)
        assertTrue(result.record.isEmpty())

        this.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun updateMultipleRecords() {
        val dbFile = this.dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        // Update multiple records
        val keys = listOf(
            buildMunicipalityKey("IT", "LOM", "BS", "BIENNO"),
            buildMunicipalityKey("IT", "LOM", "BS", "BIONE")
        )

        keys.forEach { key ->
            val chainResult = dbFile.chain(key)
            if (chainResult.record.isNotEmpty()) {
                val record = chainResult.record
                record.set("PROV", "BA")
                dbFile.update(record)
            }
        }

        val newKeys = listOf(
            buildMunicipalityKey("IT", "LOM", "BA", "BIENNO"),
            buildMunicipalityKey("IT", "LOM", "BA", "BIONE")
        )

        // Verify all records are updated
        newKeys.forEach { key ->
            val chainResult = dbFile.chain(key)
            assertEquals("BA", getMunicipalityProv(chainResult.record))
        }

        this.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }


    @Test
    fun chainDeleteChain() {
        val dbFile = this.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult2 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "CREDARO"))
        if(chainResult2.record.isNotEmpty()){
            assertEquals("CREDARO", getMunicipalityName(chainResult2.record))
            dbFile.delete(chainResult2.record)
            dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "CREDARO"))
        }
        this.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun readDeleteRead() {
        val dbFile = this.dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BG", "COVO")))
        val record = dbFile.read().record
        dbFile.delete(record)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BG", "COVO")))
        dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BG", "COVO"))
        assertTrue(dbFile.eof())
        this.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun updateUpsertTrue() {
        val dbFile = this.dbManager.openFile(MUNICIPALITY_TABLE_NAME)

        // Chain to record to modify
        var chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "DALMINE"))
        if(chainResult.record.isNotEmpty()){
            assertEquals("DALMINE", getMunicipalityName(chainResult.record))
            assertEquals("BG", getMunicipalityProv(chainResult.record))

            // Update
            val record = chainResult.record
            record.set("PROV", "BS")
            dbFile.update(record)

            // Verify changes
            chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "DALMINE"))
            assertEquals("BS", getMunicipalityProv(chainResult.record))

            // Verify that old record is still there
            chainResult = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BG", "DALMINE"))
            assertTrue(chainResult.record.isNotEmpty())
        }
        this.dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }


}