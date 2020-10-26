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

package com.smeup.reload.metadata.file

import com.smeup.reload.model.Field
import com.smeup.reload.model.FileMetadata
import com.smeup.reload.utils.getFieldTypeInstance
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.util.*
import kotlin.collections.ArrayList


object PropertiesSerializer {

    fun propertiesToMetadata(propertiesDirPath: String, fileName: String): FileMetadata{
        val propertiesFile = FileInputStream(File("$propertiesDirPath${File.separatorChar}${fileName.toUpperCase()}.properties"))
        //val properties = Properties()
        //properties.load(InputStreamReader(propertiesFile, Charset.forName("UTF-8")))

        val mp: MutableMap<String, String> = LinkedHashMap()
        object : Properties() {
            @Synchronized
            override fun put(key: Any, value: Any): Any? {
                return mp.put(key as String, value as String)
            }
        }.load(InputStreamReader(propertiesFile, Charset.forName("UTF-8")))


        // FormatName
        val recordFormat = mp.get("recordformat")!!

        // Fields
        var flds = mp.filterKeys { it.toString().startsWith("field.") }
        val fields: MutableList<Field> = ArrayList()
        flds.forEach { fld ->
            val name = fld.key.split(".")[1]
            val fldAttributes = fld.value.split(",")
            val description = fldAttributes[0].trim()
            val length = fldAttributes[2].trim().toInt()
            val decimal = fldAttributes[3].trim().toInt()

            val datatype = fldAttributes[1].trim()
            val fieldType = datatype.getFieldTypeInstance(length, decimal)

            fields.add(Field(name, fieldType, false, description))
        }

        // FieldKeys
        val fieldsKeys: MutableList<String> = ArrayList()
        if(!(mp.get("filekeys")).isNullOrEmpty()){
            fieldsKeys.addAll((mp.get("filekeys")?.split(",")!!))
        }

        // Unique
        val unique = mp.get("unique")!!.toBoolean()

        return FileMetadata(fileName, recordFormat, fields, fieldsKeys, unique)
    }

    fun metadataToProperties(propertiesDirPath: String, fileMetadata: FileMetadata, overwrite: Boolean){

        val properties = mutableListOf<Pair<String, String>>()

        properties.add(Pair("recordformat", fileMetadata.recordFormat))

        for (field in fileMetadata.fields) {
            properties.add(
                Pair(
                    "field.${field.name}",
                    "${field.text},${field.type.type.name},${field.type.size},${field.type.digits}"
                )
            )
        }

        var keys = "${fileMetadata.fileKeys.joinToString(",")}"
        properties.add(Pair("filekeys", keys))

        var unique = "false"
        if (fileMetadata.unique) unique = "true"
        properties.add(Pair("unique", unique))

        val propertiesFilePath = "${propertiesDirPath}${File.separatorChar}${fileMetadata.tableName.toUpperCase()}.properties"

        val propertiesFile = File(propertiesFilePath)

        if (overwrite && propertiesFile.exists())  {
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
