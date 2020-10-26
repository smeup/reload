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

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.utils.getFieldTypeInstance
import org.bson.Document

fun FileMetadata.toMongoDocument(): Document {

    val metadataObject = Document("name", tableName)

    // formatName
    metadataObject.put("format", this.recordFormat)

    // Fields
    val fieldsDoc = mutableListOf<Document>()

    this.fields.forEach {
        val fieldObjectDocument = Document("name", it.name)

        val fieldTypeDocument = Document()
        fieldTypeDocument.put("type",  it.type.type.toString())
        fieldTypeDocument.put("size",  it.type.size)
        fieldTypeDocument.put("digits", it.type.digits)
        fieldObjectDocument.put("type", fieldTypeDocument)
        fieldObjectDocument.put("notnull", it.notnull)
        fieldObjectDocument.put("text", it.text)
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

    // Unique
    metadataObject.put("unique", this.unique)

    return metadataObject
}

fun Document.toMetadata(): FileMetadata {
    // Name
    val name = get("name") as String

    val formatName = get("format") as String

    // Fields
    val fields = getList("fields", Document::class.java)
    val fieldsList = mutableListOf<Field>()

    fields.forEach { item ->

        val fieldName  = item.getString("name")

        // Build FieldType object
        val typeAsDocument = item.get("type") as Document

        val type =  typeAsDocument.getString("type")
        val size =  typeAsDocument.getInteger("size")
        val digits =  typeAsDocument.getInteger("digits")
        val typeFieldObject = type.getFieldTypeInstance(size, digits)
        val notnull = item.get("notnull") ?: false
        val text = typeAsDocument.getString("text")

        val field = Field(fieldName, typeFieldObject, notnull as Boolean, text)
        fieldsList.add(field)
    }

    //Keys
    val keys = getList("fields", Document::class.java)
    val keysList = mutableListOf<String>()

    keys.forEach { item ->

        val key  = item.getString("name")
        keysList.add(key)
    }


    val unique = get("unique") as Boolean

    return FileMetadata(name, formatName, fieldsList, keysList, unique)
}

fun FileMetadata.buildInsertCommand(filename: String, record: Record): String {
    //TODO: insert controls beetwen metadata and record format
    println("Build insert command from ${this.tableName} metadata")

    val documents = StringBuilder()

    record.toList().joinTo(documents, separator=",", prefix="{", postfix="}") {
        "${it.first}: \"${it.second}\""
    }

    val result = """
    {
        insert: "${filename.toUpperCase()}",
        documents: [ $documents ]
    }
    """

    println(result)

    return result

}


/*
Expected command format:

{
    createIndexes: "collection_name",
    indexes: [
        {
            key: {
                key1: 1,
                key2: 1,
                key3: 1
            },
            name: "index_model_name_1",
            unique: true
        },
        {
            key: {
                key1: 1,
                key2: 1
            },
            name: "index_model_name_2",
            unique: true
        }
    ],
    writeConcern: { w: "majority" }
  }

 */

fun FileMetadata.buildIndexCommand(): String{

    val keys = StringBuilder()

    this.fileKeys.joinTo(keys, separator=",", prefix="{", postfix="}") {
        "${it}: 1"
    }

    val result = """
    {
        createIndexes: "${this.tableName.toUpperCase()}",
        indexes: [
        {
            key: ${keys},
            name: "${this.tableName.toUpperCase()}_index",
            unique: false
        }
        ],
        writeConcern: { w: "majority" }
    }
    """
    println(result)

    return result

}

