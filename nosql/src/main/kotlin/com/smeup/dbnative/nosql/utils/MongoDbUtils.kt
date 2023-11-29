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
import java.util.*

fun FileMetadata.buildInsertCommand(
    filename: String,
    record: Record,
): String {
    // TODO: insert controls beetwen metadata and record format
    println("Build insert command from $filename metadata")

    val documents = StringBuilder()

    record.toList().joinTo(documents, separator = ",", prefix = "{", postfix = "}") {
        "\"${it.first}\": \"${it.second}\""
    }

    val result = """
    {
        insert: "${filename.uppercase(Locale.getDefault())}",
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

fun FileMetadata.buildIndexCommand(): String {
    val keys = StringBuilder()

    this.fileKeys.joinTo(keys, separator = ",", prefix = "{", postfix = "}") {
        "\"${it}\": 1"
    }

    val result = """
    {
        createIndexes: "${this.tableName.uppercase(Locale.getDefault())}",
        indexes: [
        {
            key: $keys,
            name: "${this.tableName.uppercase(Locale.getDefault())}_index",
            unique: false
        }
        ],
        writeConcern: { w: "majority" }
    }
    """
    println(result)

    return result
}
