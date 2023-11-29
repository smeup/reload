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

package com.smeup.dbnative.nosql.utils

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingLevel
import com.smeup.dbnative.model.*
import com.smeup.dbnative.nosql.NoSQLDBMManager
import com.smeup.dbnative.utils.TypedField
import com.smeup.dbnative.utils.TypedMetadata
import com.smeup.dbnative.utils.fieldByType
import org.bson.Document
import org.junit.Assert
import java.io.File

const val TSTAB_TABLE_NAME = "TSTAB01"
const val MUNICIPALITY_TABLE_NAME = "MUNICIPALITY"
const val TEST_LOG = false
private val LOGGING_LEVEL = LoggingLevel.OFF

fun dbManagerForTest(): NoSQLDBMManager {
    testLog("Creating NOSQLDBManager with db type MONGO")
    return NoSQLDBMManager(ConnectionConfig("*", "mongodb://localhost:27017/W_TEST", "", "")).apply {
        logger = Logger.getSimpleInstance(LOGGING_LEVEL)
    }
}

fun createAndPopulateTestTable(dbManager: NoSQLDBMManager) {
    // Create file
    val fields =
        listOf(
            "TSTFLDCHR" fieldByType CharacterType(3),
            "TSTFLDNBR" fieldByType DecimalType(5, 2),
        )

    val keys =
        listOf(
            "TSTFLDCHR",
        )
    val tMetadata = TypedMetadata(TSTAB_TABLE_NAME, "TSTREC", fields, keys)
    createFile(tMetadata, dbManager)

    Assert.assertTrue(dbManager.existFile(tMetadata.name))
    Assert.assertTrue(dbManager.metadataOf(tMetadata.name) == tMetadata.fileMetadata())

    val dbfile: DBFile? = dbManager.openFile(tMetadata.name)
    dbfile!!.write(Record(RecordField("TSTFLDCHR", "XXX"), RecordField("TSTFLDNBR", "123.45")))
    dbManager.closeFile(tMetadata.name)
}

fun createAndPopulateMunicipalityTable(dbManager: NoSQLDBMManager) {
    if (!dbManager.existFile(MUNICIPALITY_TABLE_NAME)) {
        val fields =
            listOf(
                "£NAZ" fieldByType CharacterType(2),
                "§REG" fieldByType CharacterType(3),
                "PROV" fieldByType CharacterType(2),
                "CITTA" fieldByType VarcharType(35),
                "CAP" fieldByType CharacterType(5),
                "PREF" fieldByType CharacterType(4),
                "COMUNE" fieldByType CharacterType(4),
                "ISTAT" fieldByType CharacterType(6),
            )

        val keys =
            listOf(
                "£NAZ",
                "§REG",
                "PROV",
                "CITTA",
            )
        createAndPopulateTable(
            dbManager,
            MUNICIPALITY_TABLE_NAME,
            fields,
            keys,
            "src/test/resources/csv/Municipality.csv",
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

private fun createAndPopulateTable(
    dbManager: NoSQLDBMManager,
    tableName: String,
    fields: List<TypedField>,
    keys: List<String>,
    dataFilePath: String,
) {
    val tMetadata = TypedMetadata(tableName, tableName, fields, keys)

    // if not exist file on mongodb create and populate with data
    if (dbManager.existFile(tableName) == false) {
        createFile(tMetadata, dbManager)
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
        // If table already exist in MongoDB, only registered metadata
        dbManager.registerMetadata(tMetadata.fileMetadata(), true)
    }

    dbManager.closeFile(tableName)
}

fun createFile(
    tMetadata: TypedMetadata,
    dbManager: NoSQLDBMManager,
) {
    // Find table registration in library metadata file
    val metadata: FileMetadata = tMetadata.fileMetadata()

    if (dbManager.existFile(metadata.name) == false) {
        // Create file index
        dbManager.mongoDatabase.runCommand(Document.parse(metadata.buildIndexCommand()))
    }
    dbManager.registerMetadata(metadata, true)
}
