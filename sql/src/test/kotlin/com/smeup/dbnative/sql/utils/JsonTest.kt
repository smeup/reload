/*
 *  Copyright 2024 The Reload project Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.smeup.dbnative.sql.utils

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.io.File

data class Field(
    val name: String,
    val text: String = "",
    val numeric: Boolean = false
)

data class FileMetadata(
    var name: String,
    var tableName: String,
    var fields: List<Field>,
    var fileKeys: List<String>
)

fun main() {
    // Create an instance of FileMetadata
    val fileMetadata = FileMetadata(
        "example.txt",
        "ExampleTable",
        listOf(
            Field("field1", "some text", false),
            Field("field2", numeric = true)
        ),
        listOf("key1", "key2")
    )

    // Convert FileMetadata object to JSON
    val gson: Gson = GsonBuilder().setPrettyPrinting().create()
    val json: String = gson.toJson(fileMetadata)

    // Write JSON to a file
    val outputFile = File("file_metadata.json")
    outputFile.writeText(json)

    System.out.println(json);

    println("File metadata has been written to file_metadata.json")
}
