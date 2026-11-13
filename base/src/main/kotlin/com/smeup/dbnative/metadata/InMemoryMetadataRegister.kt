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


package com.smeup.dbnative.metadata

import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.model.copyForOpen
import java.util.concurrent.ConcurrentHashMap

/**
 * Metadata register kept in memory, keyed by file name (case insensitive). It is the default register:
 * nothing is written to disk, so registrations do not outlive the JVM and are not shared between JVMs.
 *
 * It only serves files opened without metadata (`DBMManager.openFile(name)`): a file opened with its
 * metadata (`DBMManager.openFile(name, metadata)`, as `DBFileFactory.open(name, metadata)` does) never
 * goes through the register.
 */
class InMemoryMetadataRegister : MetadataRegister {

    private val metadata = ConcurrentHashMap<String, FileMetadata>()

    override fun registerMetadata(metadata: FileMetadata, overwrite: Boolean) {
        val key = metadata.name.uppercase()
        val copy = metadata.copyForOpen(key)
        if (overwrite) this.metadata[key] = copy else this.metadata.putIfAbsent(key, copy)
    }

    override fun getMetadata(filename: String): FileMetadata {
        val registered = metadata[filename.uppercase()]
            ?: throw NoSuchElementException("No metadata registered for file $filename")
        return registered.copyForOpen(filename)
    }

    override fun contains(fileName: String): Boolean = metadata.containsKey(fileName.uppercase())

    override fun remove(fileName: String) {
        metadata.remove(fileName.uppercase())
    }
}
