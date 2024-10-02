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

package com.smeup.dbnative.metadata.memory

import com.smeup.dbnative.metadata.MetadataRegister
import com.smeup.dbnative.model.FileMetadata

object MemoryMetadataRegisterImpl:MetadataRegister {

    // HashMap to store Metadata objects with their ID as the key
    private val metadataMap: HashMap<String, FileMetadata> = HashMap()

    override fun registerMetadata(metadata: FileMetadata, overwrite: Boolean) {
        if (overwrite || !metadataMap.containsKey(metadata.name.lowercase())) {
            metadataMap[metadata.name.lowercase()] = metadata
        }
    }

    override fun getMetadata(filename: String): FileMetadata? {
        return metadataMap[filename.lowercase()]
    }

    override fun contains(fileName: String): Boolean {
        return metadataMap.contains(fileName.lowercase())
    }

    override fun remove(fileName: String) {
        metadataMap.remove(fileName.lowercase())
    }
}