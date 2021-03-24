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

package com.smeup.dbnative.jt400

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.metadata.file.PropertiesSerializer
import com.smeup.dbnative.model.DecimalType
import com.smeup.dbnative.jt400.utils.createAndPopulateMunicipalityTable
import com.smeup.dbnative.jt400.utils.dbManagerForTest
import com.smeup.dbnative.jt400.utils.destroyDatabase
import com.smeup.dbnative.utils.propertiesToTypedMetadata
import org.junit.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JT400OperationsOnFile {

    private lateinit var dbManager: JT400DBMMAnager

    @Before
    fun setUp() {
        println("setup")
        dbManager = dbManagerForTest()
        createAndPopulateMunicipalityTable(dbManager)
    }

    @After
    fun tearDown() {
        println("tearDown")
        destroyDatabase()
        dbManager.close()
    }

    @Test
    fun open() {
        var fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0F")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("BRARTI0F")
        assertEquals(115, dbFile.fileMetadata.fields.size)
        dbManager.closeFile("BRARTI0F")

        fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG0L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile2 = dbManager.openFile("VERAPG0L")
        assertEquals(68, dbFile2.fileMetadata.fields.size)
        dbManager.closeFile("VERAPG0L")
    }

    @Test
    fun findRecordsIfSetllAndReadEWithKeyExistingKey() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI2L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("BRARTI2L")

        val key = "ART  "
        val keyList = listOf(key)

        assertTrue(dbFile.setll(keyList))

        var readEResult = dbFile.readEqual(keyList)
        assertEquals("2  ", readEResult.record["A§TPAR"])
        assertEquals("ACCENSIONE PIEZOELETTRICA          ", readEResult.record["A§DEAR"])

        readEResult = dbFile.readEqual(keyList)
        assertEquals("1  ", readEResult.record["A§TPAR"])
        assertEquals("ARTICOLO PER PARALLELISMO          ", readEResult.record["A§DEAR"])

        readEResult = dbFile.readEqual(keyList)
        assertEquals("1  ", readEResult.record["A§TPAR"])
        assertEquals("BIKE (PROVA                        ", readEResult.record["A§DEAR"])

        dbManager.closeFile("BRARTI2L")
    }

    @Test
    fun updateRecordAfterSingleKeyChain() {
        // TEST FLOW
        // Step1: check record exists
        // Step2: modify field and update
        // Step3: check record is updated
        // Step4: restore previous field value and update
        // Step5: check record is updated (value must be as initial)
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("BRARTI0L")

        val itemFieldKey = "A02            "
        val keyList = listOf(itemFieldKey)

        var chainResult = dbFile.chain(keyList)

        // Check chain result as expected
        assertEquals(itemFieldKey, chainResult.record["A§ARTI"])
        assertEquals("GRUPPO CAMBIO-PIGNONE              ", chainResult.record["A§DEAR"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("2  ", chainResult.record["A§TPAR"])

        // Change some current recordfields values (NOT the key), then update
        chainResult.record["A§TIAR"] = "TRA  "
        chainResult.record["A§TPAR"] = "1  "
        dbFile.update(chainResult.record)

        // Chain again (same single key) and check data as expected
        chainResult = dbFile.chain(keyList)
        assertEquals("TRA  ", chainResult.record["A§TIAR"])
        assertEquals("1  ", chainResult.record["A§TPAR"])

        // Restore initial values (chain not required)
        chainResult.record["A§TIAR"] = "ART  "
        chainResult.record["A§TPAR"] = "2  "
        dbFile.update(chainResult.record)

        // Check the initial values restore
        chainResult = dbFile.chain(keyList)
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("2  ", chainResult.record["A§TPAR"])

        dbManager.closeFile("BRARTI0L")
    }

    @Test
    fun writeNotExistingRecord() {
        // TEST FLOW
        // Step1: check record not exists
        // Step2: set fields and write
        // Step3: check the new record exists
        // Step4: delete written record
        // Step5: check record not exists (DB must be as initial)
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("BRARTI0L")
        val key = System.currentTimeMillis().toString() + "  "
        val keyList = listOf(key)
        var chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        // Set field values and write record
        chainResult.record["A§ARTI"] = key
        chainResult.record["A§DEAR"] = "Kotlin DBNativeAccess Write        "
        chainResult.record["A§TIAR"] = "ART  "
        chainResult.record["A§DT01"] = "20200918"
        dbFile.write(chainResult.record)

        // Check correct write
        chainResult = dbFile.chain(keyList)
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("Kotlin DBNativeAccess Write        ", chainResult.record["A§DEAR"])
        assertEquals("20200918", chainResult.record["A§DT01"])

        // Delete record
        dbFile.delete(chainResult.record)

        // Check record not exists
        chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        dbManager.closeFile("BRARTI0L")
    }

    @Test
    fun deleteExistingRecord() {
        // TEST FLOW
        // Step1: check record not exists
        // Step2: set fields and write
        // Step3: check the new record exists
        // Step4: delete written record
        // Step5: check record not exists (DB must be as initial)
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("BRARTI0L")
        val key = System.currentTimeMillis().toString() + "  "
        val keyList = listOf(key)

        // Not exists
        var chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        //Set field values and write record
        chainResult.record["A§ARTI"] = key
        chainResult.record["A§DEAR"] = "Kotlin DBNativeAccess Write        "
        chainResult.record["A§TIAR"] = "ART  "
        chainResult.record["A§TPAR"] = "0  "

        dbFile.write(chainResult.record)

        //Must exists correct write
        chainResult = dbFile.chain(keyList)
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("Kotlin DBNativeAccess Write        ", chainResult.record["A§DEAR"])
        assertEquals("0  ", chainResult.record["A§TPAR"])

        //Delete record
        dbFile.delete(chainResult.record)

        //Check delete success
        chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        dbManager.closeFile("BRARTI0L")
    }

    @Test
    fun multipleUpdateOnReadE(){
        // TEST FLOW
        // Step1: write 100 records with "currentTimeMillis" as unique key
        // Step2: read above written records and update A§DEA2 field
        // Step3: check and check for correct update
        // Step4: delete written record.
        val tMetadata = propertiesToTypedMetadata("src/test/resources/dds/properties/", "BRARTI1L")
        dbManager.registerMetadata(tMetadata.fileMetadata(), false)
        val dbFile = dbManager.openFile("BRARTI1L")

        // Number of record this test work with (write, update and delete)
        val numberOfRecordsToHandle = 10

        // Create list of items to write into A§ARTI field
        val items = mutableListOf<String>()
        repeat(numberOfRecordsToHandle){
            items.add(System.currentTimeMillis().toString() + "  ")
            Thread.sleep(5)
        }

        val fieldsNumber = tMetadata.fields.size
        val empty35char = "                                   "
        val dearKey = "Kotlin DBNativeAccess TEST         "
        val dea2Key = "Kotlin DBNativeAccess TEST-UPDATED "

        // WRITE
        repeat(numberOfRecordsToHandle){
            val record = Record()
            repeat(fieldsNumber){ index ->
                val name: String = dbFile.fileMetadata.fields[index].name
                //print(dbFile.fileMetadata.getField(name)?.type)
                val value = when(name){
                    "A§ARTI" -> items[it]
                    "A§DEAR" -> dearKey
                    else -> when(tMetadata.getField(name)?.type){
                        is DecimalType -> "0"
                        else -> ""
                    }
                }

                val recordField = RecordField(name, value)
                record.add(recordField)
            }
            dbFile.write(record)
        }

        // READ AND UPDATE
        // Read records with same description (A§DEAR) and update field named 'secondary description' (A§DEA2)
        val keyList = listOf(dearKey)
        assertTrue(dbFile.setll(keyList))
        //dbFile.positionCursorBefore(keyList) //TODO rivedere
        // Update
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(keyList)
            assertEquals(dearKey, readEResult.record["A§DEAR"])
            assertEquals(empty35char, readEResult.record["A§DEA2"])
            readEResult.record["A§DEA2"] = dea2Key
            dbFile.update(readEResult.record)
        }

        // READ AND CHECK
        // Check all records are updated as expected
        assertTrue(dbFile.setll(keyList))
        //dbFile.positionCursorBefore(keyList)  //TODO rivedere
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(keyList)
            println("[READ AND CHECK]: " + readEResult.record["A§ARTI"])
            assertEquals(dea2Key, readEResult.record["A§DEA2"])
        }

        // DELETE
        assertTrue(dbFile.setll(keyList))
        //dbFile.positionCursorBefore(keyList)  //TODO rivedere
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(keyList)
            assertEquals(dea2Key, readEResult.record["A§DEA2"])
            //Delete record
            dbFile.delete(readEResult.record)
        }

    }

    @Test
    fun writeNotExistingRecordOnAHugeTable() {
        // TEST FLOW (use a table with big amount of data, at lease >1.0M records)
        // Step1: check record not exists
        // Step2: set fields and write
        // Step3: check the new record exists
        // Step4: delete written record
        // Step5: check record not exists (DB must be as initial)

        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG0L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("VERAPG0L")
        val key = System.currentTimeMillis().toString().substring(3)
        val keyList = listOf(key)
        var chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        // Set field values and write record
        chainResult.record["V£IDOJ"] = key
        chainResult.record["V£NOME"] = "DBNativeAccess "
        dbFile.write(chainResult.record)

        // Check correct write
        chainResult = dbFile.chain(keyList)
        assertEquals(key, chainResult.record["V£IDOJ"])
        assertEquals("DBNativeAccess ", chainResult.record["V£NOME"])

        // Delete record
        dbFile.delete(chainResult.record)

        // Check record not exists
        chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        dbManager.closeFile("VERAPG0L")
    }

    @Test
    fun findRecordsIfSetllAndReadWithKeyExistingKey() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG1L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("VERAPG1L")

        //keys: V£DATA, V£NOME, V£IDOJ
        val data = "20200901"
        val nome = "BNUNCA         "
        val idoj = "0002003070"
        val keyList = listOf(data, nome, idoj)
        assertTrue(dbFile.setll(keyList))

        var readResult = dbFile.read()
        assertEquals("20200901", readResult.record["V£DATA"])
        assertEquals("BOLPIE         ", readResult.record["V£NOME"])
        assertEquals("0002003070", readResult.record["V£IDOJ"])

        readResult = dbFile.read()
        assertEquals("20200901", readResult.record["V£DATA"])
        assertEquals("BOLPIE         ", readResult.record["V£NOME"])
        assertEquals("0002003106", readResult.record["V£IDOJ"])

        readResult = dbFile.read()
        assertEquals("20200901", readResult.record["V£DATA"])
        assertEquals("BOLPIE         ", readResult.record["V£NOME"])
        assertEquals("0002003108", readResult.record["V£IDOJ"])

        dbManager.closeFile("VERAPG1L")
    }

    @Test
    fun findRecordsIfChainWithExistingKey() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("BRARTI0L")
        val key = "A01            "
        val keyList = listOf(key)
        val chainResult = dbFile.chain(keyList)
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("1  ", chainResult.record["A§TPAR"])
        dbManager.closeFile("BRARTI0L")
    }

    /*
    @Test
    fun resultSetCursorMovements(){
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("BRARTI0L")

        val key = "A01            "
        val keyList = listOf(RecordField("A§ARTI", key))
        // Fill ResultSet
        assertTrue(dbFile.setll(keyList))

        var resultSet = dbFile.getResultSet()

        // Cursor to 1st row
        resultSet?.first()
        assertEquals(resultSet?.getString("A§ARTI"), "A01            ")

        // Cursor to last row
        resultSet?.last()
        assertEquals(resultSet?.getString("A§ARTI"), "89807-04       ")

        dbManager.closeFile("BRARTI0L")
    }

    @Test
    fun resultSetCursorUpdate(){
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager.registerMetadata(fileMetadata, false)
        val dbFile = dbManager.openFile("BRARTI0L")

        val key = "A01            "
        val keyList = listOf(RecordField("A§ARTI", key))

        // Fill ResultSet
        assertTrue(dbFile.setll(keyList))

        var resultSet = dbFile.getResultSet()

        // Cursor to 1st row
        resultSet?.first()
        assertEquals(resultSet?.getString("A§ARTI"), "A01            ")

        resultSet?.first()
        assertEquals(resultSet?.getString("A§ARTI"), "A01            ")

        // Update record
        resultSet?.updateString("A§TIAR", "XXXXX")
        resultSet?.updateRow()

        // Move Cursor to last row
        resultSet?.last()
        assertEquals(resultSet?.getString("A§ARTI"), "89807-04       ")

        // Move Cursor to 1st row, the record should contain updated value
        resultSet?.first()
        assertEquals(resultSet?.getString("A§TIAR"), "XXXXX")

        // Restore initial field value
        resultSet?.updateString("A§TIAR", "ART  ")
        resultSet?.updateRow()

        dbManager.closeFile("BRARTI0L")
    }
     */

}

