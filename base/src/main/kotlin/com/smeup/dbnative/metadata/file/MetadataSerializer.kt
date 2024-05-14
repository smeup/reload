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

package com.smeup.dbnative.metadata.file

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.smeup.dbnative.model.FileMetadata
import java.io.File
import java.nio.charset.Charset


object MetadataSerializer {

    fun jsonToMetadata(directory: String, fileName: String): FileMetadata{
        // Read JSON data from file
        val inputFile = File(directory, fileName.uppercase() + ".json")
        val json: String = inputFile.readText()

        // Deserialize
        val gson = Gson()
        val fileMetadata: FileMetadata = gson.fromJson(json, FileMetadata::class.java)

        // if fileName field is undefined, set it with metadata file name
        if (fileMetadata.tableName.isEmpty()) {
            fileMetadata.tableName = fileName
        }
        return fileMetadata
    }

    fun metadataToJson(directory: String, fileMetadata: FileMetadata, overwrite: Boolean){
        // Convert FileMetadata object to JSON
        val gson: Gson = GsonBuilder().setPrettyPrinting().create()
        val json: String = gson.toJson(fileMetadata)

        // Write JSON to file
        val propertiesFilePath = "${directory}${File.separatorChar}${fileMetadata.name.uppercase()}.json"
        val outputFile = File(propertiesFilePath)
        if (overwrite && outputFile.exists()) {
            outputFile.delete()
        }
        outputFile.writeText(json)
    }
}
