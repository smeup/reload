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

import com.smeup.dbnative.log.Logging
import com.smeup.dbnative.model.FileMetadata

/**
 * DBFile is an abstraction of table or view (in sql database) or document (in nosql database).
 * */
interface DBFile: AutoCloseable, Logging {

    var name: String
    var fileMetadata: FileMetadata

    // Control functions
    /**
     * Used to detect end-of file, beginning of file, or subfile full conditions while performing a file operation similar to resulting indicator
     * @return '1' if end-of file, beginning of file, or subfile full condition is found ; otherwise, it returns '0'.
     */
    fun eof(): Boolean

    /**
     * It is used by the SETLL operation to indicate that it detected a record in the file with a key equal to that of the value specified
     */
    fun equal(): Boolean

    // Pointing functions
    /**
     * Sets the file pointer at the first occurrence of the record where the key field value is greater than or equal to the factor-1 search argument value.
     */
    fun setll(key: String): Boolean
    fun setll(keys: List<String>): Boolean

    /**
     * Positions the file pointer at the next record which is having the key value just greater than the current key value.
     */
    fun setgt(key: String): Boolean
    fun setgt(keys: List<String>): Boolean


    // Read functions
    /**
     * The CHAIN command does a SETLL and a READE in order to find a match. CHAIN is best used to locate a unique record (like a customer record) from a full procedural file
     */
    fun chain(key: String): Result
    fun chain(keys: List<String>): Result

    /**
     *  Read operation reads the records of a full procedural file. First of all it reads the record where currently the pointer is and then advances the pointer to the next record.
     */
    fun read(): Result

    /**
     * READP moves the pointer to the previous record and reads the record and again moves the pointer to next previous position. If there are no more records it sets EOF *ON
     */
    fun readPrevious(): Result

    /**
     * READE reads the matching record for factor-1 and moves the pointer to the next record with the same matching criteria. If the same matching criteria is not found then it foes to EOF.
     */
    fun readEqual(): Result
    fun readEqual(key: String): Result
    fun readEqual(keys: List<String>): Result

    /**
     * READPE moves the pointer to the previous record and reads the record and again moves the pointer to next previous position and read the same matching record for factor-1
     */
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