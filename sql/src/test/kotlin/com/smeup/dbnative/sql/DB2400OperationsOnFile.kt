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
import com.smeup.dbnative.metadata.file.PropertiesSerializer
import com.smeup.dbnative.model.DecimalType
import com.smeup.dbnative.sql.utils.TestSQLDBType
import com.smeup.dbnative.sql.utils.dbManagerForTest
import com.smeup.dbnative.utils.getField
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Ignore
class DB2400OperationsOnFile {

    companion object {

        private var dbManager: SQLDBMManager? = null

        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest(TestSQLDBType.DB2_400)
        }

        @AfterClass
        fun tearDown() {
        }
    }

    @Test
    fun open() {
        var fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0F")
        dbManager!!.registerMetadata(fileMetadata, false)
        var dbFile = dbManager!!.openFile("BRARTI0F")
        assertEquals(115, dbFile.fileMetadata.fields.size)
        dbManager!!.closeFile("BRARTI0F")

        fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG0L")
        dbManager!!.registerMetadata(fileMetadata, false)
        dbFile = dbManager!!.openFile("VERAPG0L")
        assertEquals(68, dbFile.fileMetadata.fields.size)
        dbManager!!.closeFile("VERAPG0L")
    }

    @Test
    fun findRecordsIfChainWithExistingKey() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI0L")
        val key = "A01            "
        val chainResult = dbFile.chain(key)
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("1  ", chainResult.record["A§TPAR"])
        dbManager!!.closeFile("BRARTI0L")
    }

    @Test
    fun findRecordsIfSetllAndReadEWithKeyExistingKey() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI2L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI2L")

        val key = "ART  "
        assertTrue(dbFile.setll(key))

        var readEResult = dbFile.readEqual(key)
        assertEquals("2  ", readEResult.record["A§TPAR"])
        assertEquals("ACCENSIONE PIEZOELETTRICA          ", readEResult.record["A§DEAR"])

        readEResult = dbFile.readEqual(key)
        assertEquals("1  ", readEResult.record["A§TPAR"])
        assertEquals("ARTICOLO PER PARALLELISMO          ", readEResult.record["A§DEAR"])

        readEResult = dbFile.readEqual(key)
        assertEquals("1  ", readEResult.record["A§TPAR"])
        assertEquals("BIKE (PROVA                        ", readEResult.record["A§DEAR"])

        dbManager!!.closeFile("BRARTI2L")
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
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI0L")

        val key = "A02            "
        var chainResult = dbFile.chain(key)

        // Check chain result as expected
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("GRUPPO CAMBIO-PIGNONE              ", chainResult.record["A§DEAR"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("2  ", chainResult.record["A§TPAR"])

        // Change some current recordfields values (NOT the key), then update
        chainResult.record["A§TIAR"] = "TRA  "
        chainResult.record["A§TPAR"] = "1  "
        dbFile.update(chainResult.record)

        // Chain again (same single key) and check data as expected
        chainResult = dbFile.chain(key)
        assertEquals("TRA  ", chainResult.record["A§TIAR"])
        assertEquals("1  ", chainResult.record["A§TPAR"])

        // Restore initial values (chain not required)
        chainResult.record["A§TIAR"] = "ART  "
        chainResult.record["A§TPAR"] = "2  "
        dbFile.update(chainResult.record)

        // Check the initial values restore
        chainResult = dbFile.chain(key)
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("2  ", chainResult.record["A§TPAR"])

        dbManager!!.closeFile("BRARTI0L")
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
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI0L")
        val key = System.currentTimeMillis().toString() + "  "
        var chainResult = dbFile.chain(key)
        assertEquals(0, chainResult.record.size)

        // Set field values and write record
        chainResult.record["A§ARTI"] = key
        chainResult.record["A§DEAR"] = "Kotlin DBNativeAccess Write        "
        chainResult.record["A§TIAR"] = "ART  "
        chainResult.record["A§DT01"] = "20200918"
        dbFile.write(chainResult.record)

        // Check correct write
        chainResult = dbFile.chain(key)
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("Kotlin DBNativeAccess Write        ", chainResult.record["A§DEAR"])
        assertEquals("20200918", chainResult.record["A§DT01"])

        // Delete record
        dbFile.delete(chainResult.record)

        // Check record not exists
        chainResult = dbFile.chain(key)
        assertEquals(0, chainResult.record.size)

        dbManager!!.closeFile("BRARTI0L")
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
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI0L")
        val key = System.currentTimeMillis().toString() + "  "

        // Not exists
        var chainResult = dbFile.chain(key)
        assertEquals(0, chainResult.record.size)

        //Set field values and write record
        chainResult.record["A§ARTI"] = key
        chainResult.record["A§DEAR"] = "Kotlin DBNativeAccess Write        "
        chainResult.record["A§TIAR"] = "ART  "
        chainResult.record["A§TPAR"] = "0  "

        dbFile.write(chainResult.record)

        //Must exists correct write
        chainResult = dbFile.chain(key)
        assertEquals(key, chainResult.record["A§ARTI"])
        assertEquals("ART  ", chainResult.record["A§TIAR"])
        assertEquals("Kotlin DBNativeAccess Write        ", chainResult.record["A§DEAR"])
        assertEquals("0  ", chainResult.record["A§TPAR"])

        //Delete record
        dbFile.delete(chainResult.record)

        //Check delete success
        chainResult = dbFile.chain(key)
        assertEquals(0, chainResult.record.size)

        dbManager!!.closeFile("BRARTI0L")
    }

    @Test
    fun multipleUpdateOnReadE(){
        // TEST FLOW
        // Step1: write 100 records with "currentTimeMillis" as unique key
        // Step2: read above written records and update A§DEA2 field
        // Step3: check and check for correct update
        // Step4: delete written record.
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI1L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI1L")

        // Number of record this test work with (write, update and delete)
        val numberOfRecordsToHandle = 100

        // Create list of items to write into A§ARTI field
        val items = mutableListOf<String>()
        val time = System.currentTimeMillis().toInt()
        repeat(numberOfRecordsToHandle){
            items.add(( time + it).toString() + "  ")
        }

        val fieldsNumber = fileMetadata.fields.size
        val empty35char = "                                   "
        val dearKey = "Kotlin DBNativeAccess TEST         "
        val dea2Key = "Kotlin DBNativeAccess TEST-UPDATED "

        // WRITE
        repeat(numberOfRecordsToHandle){
            val record = Record()
            repeat(fieldsNumber){ index ->
                val name: String = dbFile.fileMetadata.fields[index].name
                print(dbFile.fileMetadata.getField(name)?.type)
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

        // READ AND UPDATE
        // Read records with same description (A§DEAR) and update field named 'secondary description' (A§DEA2)

        assertTrue(dbFile.setll(dearKey))
        // Update
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(dearKey)
            assertEquals(dearKey, readEResult.record["A§DEAR"])
            assertEquals(empty35char, readEResult.record["A§DEA2"])
            readEResult.record["A§DEA2"] = dea2Key
            dbFile.update(readEResult.record)
        }

        // READ AND CHECK
        // Check all records are updated as expected
        assertTrue(dbFile.setll(dearKey))
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(dearKey)
            assertEquals(dea2Key, readEResult.record["A§DEA2"])
        }

        // DELETE
        assertTrue(dbFile.setll(dearKey))
        repeat(numberOfRecordsToHandle) {
            val readEResult = dbFile.readEqual(dearKey)
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
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("VERAPG0L")
        val key = System.currentTimeMillis().toString().substring(3)

        var chainResult = dbFile.chain(key)
        assertEquals(0, chainResult.record.size)

        // Set field values and write record
        chainResult.record["V£IDOJ"] = key
        chainResult.record["V£NOME"] = "DBNativeAccess "
        dbFile.write(chainResult.record)

        // Check correct write
        chainResult = dbFile.chain(key)
        assertEquals(key, chainResult.record["V£IDOJ"])
        assertEquals("DBNativeAccess ", chainResult.record["V£NOME"])

        // Delete record
        dbFile.delete(chainResult.record)

        // Check record not exists
        chainResult = dbFile.chain(key)
        assertEquals(0, chainResult.record.size)

        dbManager!!.closeFile("VERAPG0L")
    }



    @Test
    fun failedChainOnHugeTable() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG1L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("VERAPG1L")

        //keys: V£DATA, V£NOME, V£IDOJ
        val data = "20200901"
        val nome = "WRGKEY         " // <-- Not existing key
        val keyList = listOf(data, nome)

        val readResult = dbFile.chain(keyList)

        assertTrue { readResult.indicatorHI }

        dbManager!!.closeFile("VERAPG1L")
    }

    @Test
    fun doubleChainOnHugeTable() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG1L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("VERAPG1L")

        //keys: V£DATA, V£NOME, V£IDOJ
        var data = "20200901"
        var nome = "BOLPIE         "
        var idoj = "0002003070"
        var keyList = listOf(data, nome, idoj)

        var readResult = dbFile.chain(keyList)
        assertEquals("20200901", readResult.record["V£DATA"])
        assertEquals("BOLPIE         ", readResult.record["V£NOME"])
        assertEquals("0002003070", readResult.record["V£IDOJ"])

        data = "20080102"
        nome = "MAEOLI         "
        idoj = "0000204292"
        keyList = listOf(data, nome, idoj)

        readResult = dbFile.chain(keyList)
        assertEquals("20080102", readResult.record["V£DATA"])
        assertEquals("MAEOLI         ", readResult.record["V£NOME"])
        assertEquals("0000204292", readResult.record["V£IDOJ"])

        dbManager!!.closeFile("VERAPG1L")
    }


    @Test
    fun notExactSetllOnHugeTable_1() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG1L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("VERAPG1L")

        //keys: V£DATA, V£NOME, V£IDOJ
        val data = "20201201"
        val nome = "A         "       // <-- Not existing key but first in range
        val keyList = listOf(data, nome)

        assertTrue { dbFile.setll(keyList)  }

        var result = dbFile.readEqual(listOf(data))

        dbManager!!.closeFile("VERAPG1L")
    }

    @Test
    fun notExactSetllOnHugeTable_2() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG1L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("VERAPG1L")

        //keys: V£DATA, V£NOME, V£IDOJ
        val data = "20201201"
        val nome = "Z         "     // <-- Not existing key and last in range
        val keyList = listOf(data, nome)
        assertTrue { dbFile.setll(keyList)  }

        // Next one is null, because the pointer point to last element with data 20201201 and
        // the next one has data 20201202 that don't match the first key
        val result = dbFile.readEqual(listOf(data))
        assertEquals(0, result.record.size )

        dbManager!!.closeFile("VERAPG1L")
    }

    @Test
    fun notExactSetllOnHugeTable_3() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG1L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("VERAPG1L")

        //keys: V£DATA, V£NOME, V£IDOJ
        val data = "20201201"
        val nome = "Z         "    // <-- Not existing key and last in range
        val keyList = listOf(data, nome)
        assertTrue { dbFile.setll(keyList)  }

        // Next one element, gettet wit a read command, is the first element with date 20201202.
        val result = dbFile.read()
        assertEquals("20201202", result.record["V£DATA"])

        dbManager!!.closeFile("VERAPG1L")
    }


    @Test
    fun findRecordsIfSetllAndReadWithKeyExistingKey() {
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG1L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("VERAPG1L")

        //keys: V£DATA, V£NOME, V£IDOJ
        val data = "20200901"
        val nome = "BOLPIE         "//"BNUNCA         "
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

        dbManager!!.closeFile("VERAPG1L")
    }

    @Test
    fun resultSetCursorMovements(){
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI0L")

        val key = "A01            "

        // Fill ResultSet
        assertTrue(dbFile.setll(key))

        val resultSet = dbFile.getResultSet()

        // Cursor to 1st row
        resultSet?.first()
        assertEquals(resultSet?.getString("A§ARTI"), "A01            ")

        // Cursor to last row
        resultSet?.last()
        assertEquals(resultSet?.getString("A§ARTI"), "89807-04       ")

        dbManager!!.closeFile("BRARTI0L")
    }

    @Test
    fun resultSetCursorUpdate(){
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI0L")

        val key = "A01            "

        // Fill ResultSet
        assertTrue(dbFile.setll(key))

        val resultSet = dbFile.getResultSet()

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

        dbManager!!.closeFile("BRARTI0L")
    }

    @Test
    fun simulateDataAccessLogicOfX1_X21_06N() {
        val tableName = "VERAPG9L"
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", tableName)
        dbManager!!.registerMetadata(fileMetadata, true)
        println(">Open table $tableName")
        val dbFile = dbManager!!.openFile(tableName)

        //keys: V£DATA, V£CDC
        val data = "20190215"
        val cdc = "RDSSVI         "
        var keyList = listOf(data, cdc)
        println(">SETLL with keyList $keyList")
        assertTrue(dbFile.setll(keyList))

        var readedRecord = 0
        while(true){
            println(">READE with keyList $keyList")
            /*
            val data = "20190215"
            val cdc = "ZZZZZZ         "
            keyList = listOf(data, cdc)
            */

            val readEResult = dbFile.readEqual(keyList)
            if (readEResult.record.isEmpty()) {
                break
            }
            println(">readed record ${readEResult.record}")
            readedRecord++
        }
        println(">Readed $readedRecord records")
        println(">Close table $tableName")
        dbManager!!.closeFile(tableName)
        assertEquals(6, readedRecord)
    }
}

