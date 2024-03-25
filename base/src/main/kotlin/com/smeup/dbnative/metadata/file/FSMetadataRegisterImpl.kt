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

import com.smeup.dbnative.metadata.MetadataRegister
import com.smeup.dbnative.model.FileMetadata
import java.io.File

object FSMetadataRegisterImpl: MetadataRegister{

    var propertiesDirPath: String

    init {

        propertiesDirPath = System.getenv("DBNATIVE_DDS_DIR") ?:"${System.getProperty("user.home")}${File.separatorChar}" +
                "etc${File.separatorChar}" +
                "dbnativeaccess${File.separatorChar}dds"

        if (File(propertiesDirPath).exists() == false) {
            File(propertiesDirPath).mkdirs()
        }
    }

    override fun registerMetadata(metadata: FileMetadata, overwrite: Boolean) {
        MetadataSerializer.metadataToJson(propertiesDirPath, metadata, true)
    }

    override fun getMetadata(filename: String): FileMetadata {
        return MetadataSerializer.jsonToMetadata(propertiesDirPath, filename)
    }

    override fun contains(fileName: String): Boolean {
        return File("${propertiesDirPath}${File.separatorChar}${fileName}.json").exists()
    }

    override fun remove(fileName: String) {
        var propertiesFile = File("${propertiesDirPath}${File.separatorChar}${fileName}.json")
        if (propertiesFile.exists()) propertiesFile.delete()
    }
}