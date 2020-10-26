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

package com.smeup.reload.nosql.utils

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.smeup.reload.ConnectionConfig
import com.smeup.reload.file.Record
import com.smeup.reload.file.RecordField
import com.smeup.reload.model.CharacterType
import com.smeup.reload.model.Field
import com.smeup.reload.model.FileMetadata
import com.smeup.reload.model.VarcharType
import com.smeup.reload.nosql.NoSQLDBMManager
import com.smeup.reload.utils.fieldByType
import org.junit.Assert
import java.io.File

const val MUNICIPALITY_TABLE_NAME = "MUNICIPALITY"
const val TEST_LOG = false

fun dbManagerForTest(): NoSQLDBMManager {
    testLog("Creating NOSQLDBManager with db type MONGO")
    return NoSQLDBMManager(ConnectionConfig("*", "mongodb://localhost:27017/W_TEST", "", ""))
}

fun createAndPopulateMunicipalityTable(dbManager: NoSQLDBMManager) {

    if (!dbManager.existFile(MUNICIPALITY_TABLE_NAME)) {

        val fields = listOf(
            "NAZ" fieldByType  CharacterType(2),
            "REG" fieldByType CharacterType(3),
            "PROV" fieldByType CharacterType(2),
            "CITTA" fieldByType VarcharType(35),
            "CAP" fieldByType CharacterType(5),
            "PREF" fieldByType CharacterType(4),
            "COMUNE" fieldByType CharacterType(4),
            "ISTAT" fieldByType CharacterType(6)
        )

        val keys = listOf(
            "NAZ",
            "REG",
            "PROV",
            "CITTA"
        )
        createAndPopulateTable(

            dbManager,
            MUNICIPALITY_TABLE_NAME,
            fields,
            keys,
            "src/test/resources/csv/Municipality.csv"
        )
    }
}

fun getMunicipalityName(record: Record): String {
    val name = record["CITTA"] as String
    return name.trim()
}

fun testLog(message: String) {
    if (TEST_LOG) {
        println(message)
    }
}

private fun createAndPopulateTable(dbManager: NoSQLDBMManager, tableName: String, fields: List<Field>, keys:List<String>, dataFilePath: String) {

    val metadata = FileMetadata(tableName, "TSTREC", fields, keys)

    //if not exist file on mongodb create and populate with data
    if (dbManager.existTableInMongoDB(tableName) == false) {

        dbManager.createFile(metadata)
        Assert.assertTrue(dbManager.existFile(tableName))
        var dbFile = dbManager.openFile(tableName)
        val dataFile = File(dataFilePath)
        val dataRows: List<Map<String, String>> = csvReader().readAllWithHeader(dataFile)

        dataRows.forEach {
            val recordFields = emptyList<RecordField>().toMutableList()
            it.map { (key, value) ->
                recordFields.add(RecordField(key, value))
            }
            dbFile.write(Record(*recordFields.toTypedArray()))
        }
    } else {
        // If table already exist in MongoDB, only register metadata
        dbManager.registerMetadata(metadata, true)
    }

    dbManager.closeFile(tableName)

}


