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

package com.smeup.dbnative

import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.model.FileMetadata

/**
 * Wraps a connection with "data source".
 * You can consider data source as a container of one or more files of the same type.
 * A datasource can contains either only tables(views) or only documents.
 * File is an abstraction of table, view or document.
 * */
interface DBMManager : AutoCloseable{
    val connectionConfig : ConnectionConfig


    fun existFile(name: String): Boolean
    fun registerMetadata(metadata: FileMetadata, overwrite: Boolean)
    fun metadataOf(name: String): FileMetadata
    fun createFile(metadata: FileMetadata)
    fun openFile(name: String): DBFile
    fun closeFile(name: String)
    fun unregisterMetadata(name: String)
    /**
     * Validate connectionConfig. If validation fails, implementation has to throw an IllegalArgumentException
     * */
    fun validateConfig()
}
