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

import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.utils.fieldsToProperties
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.util.*
import kotlin.collections.ArrayList

object PropertiesSerializer {
    fun propertiesToMetadata(
        propertiesDirPath: String,
        fileName: String,
    ): FileMetadata {
        val propertiesFile = FileInputStream(File("$propertiesDirPath${File.separatorChar}${fileName.uppercase()}.properties"))
        // val properties = Properties()
        // properties.load(InputStreamReader(propertiesFile, Charset.forName("UTF-8")))

        val mp: MutableMap<String, String> = LinkedHashMap()
        object : Properties() {
            @Synchronized
            override fun put(
                key: Any,
                value: Any,
            ): Any? {
                return mp.put(key as String, value as String)
            }
        }.load(InputStreamReader(propertiesFile, Charset.forName("UTF-8")))

        // Fields
        val flds = mp.filterKeys { it.startsWith("field.") }
        val fields: MutableList<Field> = ArrayList()
        flds.forEach { fld ->
            val fieldName = fld.key.split(".")[1]
            val fldAttributes = fld.value.split(",")
            val description = fldAttributes[0].trim()
            fields.add(Field(fieldName, description))
        }

        // fileName
        var tableName = mp["tablename"]!!

        // if fileName field is undefined, set it with metadata file name
        if (tableName.isEmpty()) {
            tableName = fileName
        }

        // FieldKeys
        val fieldsKeys: MutableList<String> = ArrayList()
        if (!(mp["filekeys"]).isNullOrEmpty()) {
            fieldsKeys.addAll((mp["filekeys"]?.split(",")!!))
        }

        // Create metadata
        return FileMetadata(fileName, tableName, fields, fieldsKeys)
    }

    fun metadataToProperties(
        propertiesDirPath: String,
        fileMetadata: FileMetadata,
        overwrite: Boolean,
    ) {
        metadataToPropertiesImpl(propertiesDirPath, fileMetadata, fileMetadata.fieldsToProperties(), overwrite)
    }

    private fun metadataToPropertiesImpl(
        propertiesDirPath: String,
        fileMetadata: FileMetadata,
        properties: MutableList<Pair<String, String>>,
        overwrite: Boolean,
    ) {
        properties.add(Pair("tablename", fileMetadata.tableName))

        val keys = fileMetadata.fileKeys.joinToString(",")
        properties.add(Pair("filekeys", keys))

        val propertiesFilePath = "${propertiesDirPath}${File.separatorChar}${fileMetadata.name.uppercase(Locale.getDefault())}.properties"

        val propertiesFile = File(propertiesFilePath)

        if (overwrite && propertiesFile.exists()) {
            propertiesFile.delete()
        }

        val writer = propertiesFile.bufferedWriter(Charset.forName("UTF-8"))

        properties.forEach {
            writer.append("${it.first}=${it.second}")
            writer.newLine()
        }
        writer.flush()
        writer.close()
    }
}
