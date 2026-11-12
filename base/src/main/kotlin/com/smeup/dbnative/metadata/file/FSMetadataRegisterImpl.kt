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
import java.util.concurrent.ConcurrentHashMap

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

    /**
     * What this JVM last wrote and last read for each metadata file, keyed like the file name (uppercase).
     * Programs register the metadata of their files at every open: without these caches every open deleted
     * and rewrote the json file, then parsed it back. Callers always get a copy, because FileMetadata is
     * mutable. A file changed on disk by another process is seen again only after this JVM registers or
     * removes it.
     */
    private val written = ConcurrentHashMap<String, FileMetadata>()
    private val read = ConcurrentHashMap<String, FileMetadata>()

    private fun FileMetadata.snapshot() = copy(fields = fields.toList(), fileKeys = fileKeys.toList())

    private fun jsonFile(fileName: String) = File(propertiesDirPath, fileName.uppercase() + ".json")

    override fun registerMetadata(metadata: FileMetadata, overwrite: Boolean) {
        val key = metadata.name.uppercase()
        read.remove(key)
        MetadataSerializer.metadataToJson(propertiesDirPath, metadata, true)
        written[key] = metadata.snapshot()
    }

    override fun isRegistered(metadata: FileMetadata): Boolean =
        written[metadata.name.uppercase()] == metadata && contains(metadata.name)

    override fun getMetadata(filename: String): FileMetadata {
        val key = filename.uppercase()
        val cached = read[key]
        if (cached != null && jsonFile(filename).exists()) return cached.snapshot()
        return MetadataSerializer.jsonToMetadata(propertiesDirPath, filename).also { read[key] = it.snapshot() }
    }

    override fun contains(fileName: String): Boolean {
        return File("${propertiesDirPath}${File.separatorChar}${fileName}.json").exists()
    }

    override fun remove(fileName: String) {
        written.remove(fileName.uppercase())
        read.remove(fileName.uppercase())
        var propertiesFile = File("${propertiesDirPath}${File.separatorChar}${fileName}.json")
        if (propertiesFile.exists()) propertiesFile.delete()
    }
}