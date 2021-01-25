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
import com.smeup.dbnative.utils.getField
import org.junit.*
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JT400OperationsOnFile {

    private lateinit var dbManager: JT400DBMMAnager

    @Before
    fun setUp() {
        println("setup")
        dbManager = dbManagerForTest()
        createAndPopulateMunicipalityTable(dbManager)
        println("DB variables")
        println(">FILENAME = " + dbManager.connectionConfig.fileName)
        println(">DRIVER = " + dbManager.connectionConfig.driver)
        println(">URL = " + dbManager.connectionConfig.url)
        println(">USR = " + dbManager.connectionConfig.user)
        println(">PWD = ㋡")
    }

    @After
    fun tearDown() {
        println("tearDown")
        destroyDatabase()
        dbManager.close()
    }

    @Test
    fun open() {
        println("Running an OPEN/CLOSE test")
        var tableName = "BRARTI0F"
        var nrFields = 115
        var fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)
        println(">Assert $nrFields fields")
        assertEquals(nrFields, dbFile.fileMetadata.fields.size)
        println(">Close table $tableName")
        dbManager.closeFile(tableName)

        tableName = "VERAPG0L"
        nrFields = 68
        fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile2 = dbManager.openFile(tableName)
        println(">Assert $nrFields fields")
        assertEquals(nrFields, dbFile2.fileMetadata.fields.size)
        println(">Close table $tableName")
        dbManager.closeFile(tableName)
    }

    @Test
    fun findRecordsIfSetllAndReadEWithKeyExistingKey() {
        println("Running a SETLL/READE test")
        var tableName = "BRARTI2L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)

        val key = "AR   "
        val keyList = listOf(key)

        println(">SETLL with Keylist $keyList")
        assertTrue(dbFile.setll(keyList))

        println(">READE with Keylist $keyList")
        var readEResult = dbFile.readEqual(keyList)
        assertEquals("3  ", readEResult.record["A§TPAR"])
        assertEquals("CARTUCCIA INCH.TRI-COLOUR N.344    ", readEResult.record["A§DEAR"])
        assertEquals("SVALUI    ", readEResult.record["A§USIN"])

        println(">READE with Keylist $keyList")
        readEResult = dbFile.readEqual(keyList)
        assertEquals("3  ", readEResult.record["A§TPAR"])
        assertEquals("NETGEAR ROUTER ADSL2 CON MODEM     ", readEResult.record["A§DEAR"])
        assertEquals("POIMAR    ", readEResult.record["A§USIN"])

        println(">READE with Keylist $keyList")
        readEResult = dbFile.readEqual(keyList)
        assertEquals("3  ", readEResult.record["A§TPAR"])
        assertEquals("6710B T8100 15.4 2GB 160GB         ", readEResult.record["A§DEAR"])
        assertEquals("BONVIT    ", readEResult.record["A§USIN"])

        println(">Close table $tableName")
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
        println("Running a CHAIN/UPDATE test")
        var tableName = "BRARTI0L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)

        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)

        val itemFieldKey = "dp-301pplus    "
        val keyList = listOf(itemFieldKey)

        println(">CHAIN with keyList $keyList")
        var chainResult = dbFile.chain(keyList)

        // Check chain result as expected
        assertEquals(itemFieldKey, chainResult.record["A§ARTI"])
        assertEquals("PRINT SERVER CENTRONICS-10/100 MBPS", chainResult.record["A§DEAR"])
        assertEquals("ICS  ", chainResult.record["A§TIAR"])
        assertEquals("4  ", chainResult.record["A§TPAR"])

        // Change some current recordfields values (NOT the key), then update
        chainResult.record["A§TIAR"] = "SCI  "
        chainResult.record["A§TPAR"] = "5  "
        println(">UPDATE 'A§TIAR' from 'ICS  ' to 'SCI  ' and 'A§TPAR' from '4  ' to '5  '")
        dbFile.update(chainResult.record)

        // Chain again (same single key) and check data as expected
        println(">CHAIN with keyList $keyList")
        chainResult = dbFile.chain(keyList)
        assertEquals("SCI  ", chainResult.record["A§TIAR"])
        assertEquals("5  ", chainResult.record["A§TPAR"])

        // Restore initial values (chain not required)
        chainResult.record["A§TIAR"] = "ICS  "
        chainResult.record["A§TPAR"] = "4  "
        println(">UPDATE 'A§TIAR' from 'SCI  ' to 'ICS  ' and 'A§TPAR' from '5  ' to '4  '")
        dbFile.update(chainResult.record)

        // Check the initial values restore
        println(">CHAIN with keyList $keyList")
        chainResult = dbFile.chain(keyList)
        assertEquals("ICS  ", chainResult.record["A§TIAR"])
        assertEquals("4  ", chainResult.record["A§TPAR"])

        println(">Close table $tableName")
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
        println("Running a WRITE (of non-existing record) test")
        var tableName = "BRARTI0L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)
        val key = System.currentTimeMillis().toString() + "  "
        val keyList = listOf(key)
        println(">CHAIN a non-existing record with keyList $keyList")
        var chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        // Set field values and write record
        chainResult.record["A§ARTI"] = key
        chainResult.record["A§DEAR"] = "Kotlin DBNativeAccess Write        "
        chainResult.record["A§TIAR"] = "ART  "
        chainResult.record["A§DT01"] = "20991231"
        println(">WRITE record ${chainResult.record}")
        dbFile.write(chainResult.record)

        // Check correct write
        println(">CHAIN added record ${chainResult.record}")
        chainResult = dbFile.chain(keyList)
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("Kotlin DBNativeAccess Write        ", chainResult.record["A§DEAR"])
        assertEquals("20991231", chainResult.record["A§DT01"])

        // Delete record
        println(">DELETE record ${chainResult.record}")
        dbFile.delete(chainResult.record)

        // Check record not exists
        println(">CHAIN deleted record with keyList $keyList")
        chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        println(">Close table $tableName")
        dbManager.closeFile(tableName)
    }

    @Test
    fun deleteExistingRecord() {
        // TEST FLOW
        // Step1: check record not exists
        // Step2: set fields and write
        // Step3: check the new record exists
        // Step4: delete written record
        // Step5: check record not exists (DB must be as initial)
        println("Running a DELETE (of existing record) test")
        var tableName = "BRARTI0L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)
        val key = System.currentTimeMillis().toString() + "  "
        val keyList = listOf(key)

        // Not exists
        println(">CHAIN a non-existing record with keyList $keyList")
        var chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        //Set field values and write record
        chainResult.record["A§ARTI"] = key
        chainResult.record["A§DEAR"] = "Kotlin DBNativeAccess Write        "
        chainResult.record["A§TIAR"] = "ART  "
        chainResult.record["A§TPAR"] = "0  "
        println(">WRITE record ${chainResult.record}")
        dbFile.write(chainResult.record)

        //Must exists correct write
        println(">CHAIN added record ${chainResult.record}")
        chainResult = dbFile.chain(keyList)
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("Kotlin DBNativeAccess Write        ", chainResult.record["A§DEAR"])
        assertEquals("0  ", chainResult.record["A§TPAR"])

        //Delete record
        println(">DELETE record ${chainResult.record}")
        dbFile.delete(chainResult.record)

        //Check delete success
        println(">CHAIN deleted record with keyList $keyList")
        chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        println(">Close table $tableName")
        dbManager.closeFile(tableName)
    }

    @Test
    fun multipleUpdateOnReadE(){
        // TEST FLOW
        // Step1: write 100 records with "currentTimeMillis" as unique key
        // Step2: read above written records and update A§DEA2 field
        // Step3: check and check for correct update
        // Step4: delete written record.
        println("Running a multiple UPDATE on READE test")
        var tableName = "BRARTI1L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)

        // Number of record this test work with (write, update and delete)
        val numberOfRecordsToHandle = 10

        // Create list of items to write into A§ARTI field
        val items = mutableListOf<String>()
        repeat(numberOfRecordsToHandle){
            items.add(System.currentTimeMillis().toString() + "  ")
            Thread.sleep(5)
        }

        val fieldsNumber = fileMetadata.fields.size
        val empty35char = "                                   "
        val dearKey = "Kotlin DBNativeAccess TEST         "
        val dea2Key = "Kotlin DBNativeAccess TEST-UPDATED "

        // WRITE
        println(">Write $numberOfRecordsToHandle records to work with")
        repeat(numberOfRecordsToHandle){
            val record = Record()
            repeat(fieldsNumber){ index ->
                val name: String = dbFile.fileMetadata.fields[index].name
                //print(dbFile.fileMetadata.getField(name)?.type)
                val value = when(name){
                    "A§ARTI" -> items[it]
                    "A§DEAR" -> dearKey
                    else -> when(dbFile.fileMetadata.getField(name)?.type){
                        is DecimalType -> "0"
                        else -> ""
                    }
                }

                val recordField = RecordField(name, value)
                record.add(recordField)
            }
            dbFile.write(record)
        }

        // READE AND UPDATE
        // Read records with same description (A§DEAR) and update field named 'secondary description' (A§DEA2)
        val keyList = listOf(dearKey)
        println(">SETLL with keyList $keyList")
        assertTrue(dbFile.setll(keyList))
        //dbFile.positionCursorBefore(keyList) //TODO rivedere
        // Update
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(keyList)
            println(">READE with keyList $keyList")
            assertEquals(dearKey, readEResult.record["A§DEAR"])
            assertEquals(empty35char, readEResult.record["A§DEA2"])
            println(">record: ${readEResult.record}")
            readEResult.record["A§DEA2"] = dea2Key
            println(">UPDATE with keyList $keyList")
            dbFile.update(readEResult.record)
            println(">updated record: ${readEResult.record}")
        }

        // READ AND CHECK
        // Check all records are updated as expected
        println(">SETLL with keyList $keyList")
        assertTrue(dbFile.setll(keyList))
        //dbFile.positionCursorBefore(keyList)  //TODO rivedere
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(keyList)
            println(">READ AND CHECK " + readEResult.record["A§ARTI"])
            assertEquals(dea2Key, readEResult.record["A§DEA2"])
        }

        // DELETE
        println(">SETLL with keyList $keyList")
        assertTrue(dbFile.setll(keyList))
        //dbFile.positionCursorBefore(keyList)  //TODO rivedere
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(keyList)
            assertEquals(dea2Key, readEResult.record["A§DEA2"])
            //Delete record
            println(">DELETE " + readEResult.record["A§ARTI"])
            dbFile.delete(readEResult.record)
        }

        println(">Close table $tableName")
        dbManager.closeFile(tableName)
    }

    @Test
    fun writeNotExistingRecordOnAHugeTable() {
        // TEST FLOW (use a table with big amount of data, at lease >1.0M records)
        // Step1: check record not exists
        // Step2: set fields and write
        // Step3: check the new record exists
        // Step4: delete written record
        // Step5: check record not exists (DB must be as initial)
        println("Running a WRITE (non-existing records) on a huge table")
        var tableName = "VERAPG0L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)
        val key = System.currentTimeMillis().toString().substring(3)
        val keyList = listOf(key)
        println(">CHAIN (non-existing record) with keyList $keyList")
        var chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        // Set field values and write record
        chainResult.record["V£IDOJ"] = key
        chainResult.record["V£NOME"] = "DBNativeAccess "
        println(">WRITE record $chainResult.record")
        dbFile.write(chainResult.record)

        // Check correct write
        println(">CHAIN added record with keyList $keyList")
        chainResult = dbFile.chain(keyList)
        assertEquals(key, chainResult.record["V£IDOJ"])
        assertEquals("DBNativeAccess ", chainResult.record["V£NOME"])

        // Delete record
        println(">DELETE " + chainResult.record)
        dbFile.delete(chainResult.record)

        // Check record not exists
        println(">CHAIN (non-existing record) with keyList $keyList")
        chainResult = dbFile.chain(keyList)
        assertEquals(0, chainResult.record.size)

        println(">Close table $tableName")
        dbManager.closeFile(tableName)
    }

    @Test
    fun findRecordsIfSetllAndReadWithKeyExistingKey() {
        println("Running a SETLL and READ on a huge table")
        var tableName = "VERAPG1L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)

        //keys: V£DATA, V£NOME, V£IDOJ
        val data = "20200901"
        val nome = "BNUNCA         "
        val idoj = "0002003070"
        val keyList = listOf(data, nome, idoj)
        println(">SETLL with keyList $keyList")
        assertTrue(dbFile.setll(keyList))

        println(">READ (no keylist)")
        var readResult = dbFile.read()
        println(">result ${readResult.record}")
        assertEquals("20200901", readResult.record["V£DATA"])
        assertEquals("BOLPIE         ", readResult.record["V£NOME"])
        assertEquals("0002003070", readResult.record["V£IDOJ"])

        println(">READ (no keylist)")
        readResult = dbFile.read()
        println(">result ${readResult.record}")
        assertEquals("20200901", readResult.record["V£DATA"])
        assertEquals("BOLPIE         ", readResult.record["V£NOME"])
        assertEquals("0002003106", readResult.record["V£IDOJ"])

        println(">READ (no keylist)")
        readResult = dbFile.read()
        println(">result ${readResult.record}")
        assertEquals("20200901", readResult.record["V£DATA"])
        assertEquals("BOLPIE         ", readResult.record["V£NOME"])
        assertEquals("0002003108", readResult.record["V£IDOJ"])

        println(">Close table $tableName")
        dbManager.closeFile(tableName)
    }

    @Test
    fun findRecordsIfChainWithExistingKey() {
        println("Running a CHAIN on a huge table")
        var tableName = "BRARTI0L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)
        val key = "dp-301pplus    "
        val keyList = listOf(key)
        println(">CHAIN record with keyList $keyList")
        val chainResult = dbFile.chain(keyList)
        println(">result ${chainResult.record}")
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ICS  ", chainResult.record["A§TIAR"])
        assertEquals("4  ", chainResult.record["A§TPAR"])
        println(">Close table $tableName")
        dbManager.closeFile(tableName)
    }

    @Test
    fun `simulate data access logic of rpgle X1_X21_06N`() {
        var tableName = "VERAPG9L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)

        //keys: V£DATA, V£CDC
        val data = "20190215"
        val cdc = "RDSSVI         "
        val keyList = listOf(data, cdc)
        println(">SETLL with keyList $keyList")
        assertTrue(dbFile.setll(keyList))

        var readedRecord = 0
        while(true){
            println(">READE with keyList $keyList")
            var readEResult = dbFile.readEqual(keyList)
            if (readEResult.record.isEmpty()) {
                break
            }
            println(">readed record ${readEResult.record}")
            readedRecord ++
        }
        println(">Readed $readedRecord records")
        println(">Close table $tableName")
        dbManager.closeFile(tableName)
        assertEquals(6, readedRecord)
    }

    @Test
    fun `setll and readequal by key V£CDC V£DATA`() {
        var tableName = "VERAPG3L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager.openFile(tableName)

        //keys: V£CDC, V£DATA
        val cdc = "RDSSVI         "
        val data = "20190215"
        val keyList = listOf(cdc, data)
        println(">SETLL with keyList $keyList")
        assertTrue(dbFile.setll(keyList))

        var readedRecord = 0
        while(true){
            println(">READE with keyList $keyList")
            var readEResult = dbFile.readEqual(keyList)
            if (readEResult.record.isEmpty()) {
                break
            }
            println(">readed record ${readEResult.record}")
            readedRecord ++
        }
        println(">Readed $readedRecord records")
        println(">Close table $tableName")
        dbManager.closeFile(tableName)
        assertEquals(6, readedRecord)
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

