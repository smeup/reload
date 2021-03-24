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
import com.mongodb.BasicDBObject
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
import com.smeup.dbnative.utils.getFieldTypeInstance
import org.bson.Document
import org.junit.Assert
import java.io.File

const val TSTAB_TABLE_NAME = "TSTAB01"
const val MUNICIPALITY_TABLE_NAME = "MUNICIPALITY"
const val TEST_LOG = false
private val LOGGING_LEVEL = LoggingLevel.OFF

fun dbManagerForTest(): NoSQLDBMManager {
    testLog("Creating NOSQLDBManager with db type MONGO")
    return NoSQLDBMManager(ConnectionConfig("*", "mongodb://localhost:27017/W_TEST", "", "")).apply { logger = Logger.getSimpleInstance(LOGGING_LEVEL) }
}

fun createAndPopulateTestTable(dbManager: NoSQLDBMManager) {
    // Create file
    val fields = listOf(
        "TSTFLDCHR" fieldByType CharacterType(3),
        "TSTFLDNBR" fieldByType DecimalType(5, 2)
    )

    val keys = listOf(
        "TSTFLDCHR"
    )
    val tMetadata = TypedMetadata(TSTAB_TABLE_NAME, "TSTREC", fields, keys)
    createFile(tMetadata, dbManager)

    Assert.assertTrue(dbManager.existFile(tMetadata.tableName))
    Assert.assertTrue(dbManager.metadataOf(tMetadata.tableName) == tMetadata.fileMetadata())

    val dbfile: DBFile? = dbManager.openFile(tMetadata.tableName)
    dbfile!!.write(Record(RecordField("TSTFLDCHR", "XXX"), RecordField("TSTFLDNBR", "123.45")))
    dbManager.closeFile(tMetadata.tableName)
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

private fun createAndPopulateTable(dbManager: NoSQLDBMManager, tableName: String, fields: List<TypedField>, keys:List<String>, dataFilePath: String) {

    val tMetadata = TypedMetadata(tableName, "TSTREC", fields, keys)

    //if not exist file on mongodb create and populate with data
    if (dbManager.existTableInMongoDB(tableName) == false) {

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
        // If table already exist in MongoDB, only register metadata
        dbManager.registerMetadata(tMetadata.fileMetadata(), true)
    }

    dbManager.closeFile(tableName)

}

fun createFile(tMetadata: TypedMetadata, dbManager: NoSQLDBMManager) {
    // Find table registration in library metadata file
    val metadata: FileMetadata = tMetadata.fileMetadata();
    val whereQuery = BasicDBObject()
    whereQuery.put("name", tMetadata.tableName.toUpperCase())

    val cursor = dbManager.metadataFile.find(whereQuery)

    if (cursor.count() == 0) {
        // Register file metadata
        dbManager.metadataFile.insertOne(tMetadata.toMongoDocument())
        // Create file index
        dbManager.mongoDatabase.runCommand(Document.parse(metadata.buildIndexCommand()))
    }
    dbManager.registerMetadata(metadata, true)
}

fun TypedMetadata.toMongoDocument(): Document {

    val metadataObject = Document("name", tableName)

    // formatName
    metadataObject.put("format", this.recordFormat)

    // Fields
    val fieldsDoc = mutableListOf<Document>()

    this.fields.forEach {
        val fieldObjectDocument = Document("name", it.field.name)

        val fieldTypeDocument = Document()
        fieldTypeDocument.put("type",  it.type.type.toString())
        fieldTypeDocument.put("size",  it.type.size)
        fieldTypeDocument.put("digits", it.type.digits)
        fieldObjectDocument.put("type", fieldTypeDocument)
        fieldObjectDocument.put("notnull", false)
        fieldObjectDocument.put("text", it.field.text)
        fieldsDoc.add(fieldObjectDocument)
    }
    metadataObject.put("fields", fieldsDoc)

    // fileKeys
    val keysDoc = mutableListOf<Document>()

    this.fileKeys.forEach {
        val keyObjectDocument = Document("name", it)
        keysDoc.add(keyObjectDocument)
    }
    metadataObject.put("fileKeys", keysDoc)

    return metadataObject
}

fun Document.toMetadata(): TypedMetadata {
    // Name
    val name = get("name") as String

    val formatName = get("format") as String

    // Fields
    val fields = getList("fields", Document::class.java)
    val fieldsList = mutableListOf<TypedField>()

    fields.forEach { item ->

        val fieldName  = item.getString("name")

        // Build FieldType object
        val typeAsDocument = item.get("type") as Document

        val type =  typeAsDocument.getString("type")
        val size =  typeAsDocument.getInteger("size")
        val digits =  typeAsDocument.getInteger("digits")
        val typeFieldObject = type.getFieldTypeInstance(size, digits)
        val text = typeAsDocument.getString("text")

        val field = TypedField(Field(fieldName, text), typeFieldObject)
        fieldsList.add(field)
    }

    //Keys
    val keys = getList("fields", Document::class.java)
    val keysList = mutableListOf<String>()

    keys.forEach { item ->

        val key  = item.getString("name")
        keysList.add(key)
    }

    return TypedMetadata(name, formatName, fieldsList, keysList)
}

