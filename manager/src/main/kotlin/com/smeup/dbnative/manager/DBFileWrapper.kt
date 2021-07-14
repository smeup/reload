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

package com.smeup.dbnative.manager

import com.smeup.dbnative.DBMManager
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.FileMetadata

class DBFileWrapper (private val dbFile: DBFile, private val dbmManager: DBMManager): DBFile {

    private var closed = false

    override var name: String
        get() = dbFile.name
        set(value) {}

    override var fileMetadata: FileMetadata
        get() = dbFile.fileMetadata
        set(value) {}
    override var logger: Logger?
        get() = dbFile.logger
        set(value) {dbFile.logger = value}

    override fun eof(): Boolean {
        return dbFile.eof()
    }

    override fun equal(): Boolean {
        return dbFile.equal()
    }

    override fun setll(key: String): Boolean {
        checkClosed()
        return dbFile.setll(key)
    }

    override fun setll(keys: List<String>): Boolean {
        checkClosed()
        return dbFile.setll(keys)
    }

    override fun setgt(key: String): Boolean {
        checkClosed()
        return dbFile.setgt(key)
    }

    override fun setgt(keys: List<String>): Boolean {
        checkClosed()
        return dbFile.setgt(keys)
    }

    override fun chain(key: String): Result {
        checkClosed()
        return dbFile.chain(key)
    }

    override fun chain(keys: List<String>): Result {
        checkClosed()
        return dbFile.chain(keys)
    }

    override fun read(): Result {
        checkClosed()
        return dbFile.read()
    }

    override fun readPrevious(): Result {
        checkClosed()
        return dbFile.readPrevious()
    }

    override fun readEqual(): Result {
        checkClosed()
        return dbFile.readEqual()
    }

    override fun readEqual(key: String): Result {
        checkClosed()
        return dbFile.readEqual(key)
    }

    override fun readEqual(keys: List<String>): Result {
        checkClosed()
        return dbFile.readEqual(keys)
    }

    override fun readPreviousEqual(): Result {
        checkClosed()
        return dbFile.readPreviousEqual()
    }

    override fun readPreviousEqual(key: String): Result {
        checkClosed()
        return dbFile.readPreviousEqual(key)
    }

    override fun readPreviousEqual(keys: List<String>): Result {
        checkClosed()
        return dbFile.readPreviousEqual(keys)
    }

    override fun write(record: Record): Result {
        checkClosed()
        return dbFile.write(record)
    }

    override fun update(record: Record): Result {
        checkClosed()
        return dbFile.update(record)
    }

    override fun delete(record: Record): Result {
        checkClosed()
        return dbFile.delete(record)
    }

    private fun checkClosed() {
        require(!closed) {
            "Table ${fileMetadata.tableName} is closed"
        }
    }

    override fun close() {
        closed = true
        dbmManager.closeFile(fileMetadata.tableName)
    }
}