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
import com.smeup.dbnative.model.FileMetadata


fun FileMetadata.buildInsertCommand(filename: String, record: Record): String {
    //TODO: insert controls beetwen metadata and record format

    val documents = StringBuilder()

    record.toList().joinTo(documents, separator=",", prefix="{", postfix="}") {
        "\"${it.first}\": \"${it.second}\""
    }

    val result = """
    {
        insert: "${filename.toUpperCase()}",
        documents: [ $documents ]
    }
    """

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
        "\"${it}\": 1"
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

    return result

}

fun FileMetadata.buildCriteria(record: Record): Map<String, String> {
    val criteria = mutableMapOf<String, String>()

    this.fileKeys.forEach { key ->
        val value = record[key] ?: error("Record does not contain value for key: $key")
        criteria[key] = value
    }

    return criteria
}

fun FileMetadata.buildUpdateCommand(filename: String, record: Record, upsert:Boolean = true): String {
    val updateCriteria = this.buildCriteria(record)

    val filter = StringBuilder()
    updateCriteria.entries.joinTo(filter, separator = ",", prefix = "{", postfix = "}") {
        "\"${it.key}\": \"${it.value}\""
    }

    val updateFields = StringBuilder()
    record.toList().joinTo(updateFields, separator = ",", prefix = "{", postfix = "}") {
        "\"${it.first}\": \"${it.second}\""
    }

    val result = """
    {
        update: "${filename.toUpperCase()}",
        updates: [
            {
                q: $filter,
                u: $updateFields,
                multi: false,
                upsert: $upsert
            }
        ]
    }
    """

    return result
}

fun FileMetadata.buildDeleteCommand(filename: String, record: Record): String {
    val deleteCriteria = this.buildCriteria(record)

    val filter = StringBuilder()
    deleteCriteria.entries.joinTo(filter, separator = ",", prefix = "{", postfix = "}") {
        "\"${it.key}\": \"${it.value}\""
    }

    val result = """
    {
        delete: "${filename.toUpperCase()}",
        deletes: [
            {
                q: $filter,
                limit: 1
            }
        ]
    }
    """

    return result
}

