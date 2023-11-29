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

package com.smeup.dbnative.file

import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.FileMetadata

/**
 * DBFile is an abstraction of table or view (in sql database) or document (in nosql database).
 * */
interface DBFile : AutoCloseable {
    var name: String
    var fileMetadata: FileMetadata
    var logger: Logger?

    // Control functions
    fun eof(): Boolean

    fun equal(): Boolean

    // Pointing functions
    fun setll(key: String): Boolean

    fun setll(keys: List<String>): Boolean

    fun setgt(key: String): Boolean

    fun setgt(keys: List<String>): Boolean

    // Read functions
    fun chain(key: String): Result

    fun chain(keys: List<String>): Result

    fun read(): Result

    fun readPrevious(): Result

    fun readEqual(): Result

    fun readEqual(key: String): Result

    fun readEqual(keys: List<String>): Result

    fun readPreviousEqual(): Result

    fun readPreviousEqual(key: String): Result

    fun readPreviousEqual(keys: List<String>): Result

    // Write functions
    fun write(record: Record): Result

    // Update functions
    fun update(record: Record): Result

    // Delete functions
    fun delete(record: Record): Result

    override fun close() {}
}
