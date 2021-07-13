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

package com.smeup.dbnative.sql


import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingKey
import com.smeup.dbnative.log.NativeMethod
import com.smeup.dbnative.model.FileMetadata
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.system.measureTimeMillis

class SQLDBFile(override var fileMetadata: FileMetadata,
                var connection: Connection,
                override var logger: Logger? = null) : DBFile {

    constructor(
        fileMetadata: FileMetadata,
        connection: Connection): this(fileMetadata, connection, null)

    private var resultSet: ResultSet? = null
    private var actualRecord: Record? = null

    private var lastNativeMethod: NativeMethod? = null

    private val thisFileKeys: List<String> by lazy {
        // TODO: think about a right way (local file maybe?) to retrieve keylist
        var indexes = this.fileMetadata.fileKeys
        if(indexes.isEmpty()){
            indexes = connection.primaryKeys(fileMetadata.name)
        }
        if (indexes.isEmpty()) connection.orderingFields(fileMetadata.name) else indexes
    }
    private var adapter: Native2SQL = Native2SQL(thisFileKeys, fileMetadata.name)
    private var eof:Boolean = false

    private fun logEvent(loggingKey: LoggingKey, message: String, elapsedTime: Long? = null) =
    logger?.logEvent(loggingKey, message, elapsedTime, lastNativeMethod, fileMetadata.name)


    override fun setll(key: String): Boolean {
        return setll(mutableListOf(key))
    }

    override fun setll(keys: List<String>): Boolean {
        lastNativeMethod = NativeMethod.setll
        logEvent(LoggingKey.native_access_method, "Executing setll on keys $keys")

        adapter.setPositioning(PositioningMethod.SETLL, keys)
        return true
    }

    override fun setgt(key: String): Boolean {
        return setgt(mutableListOf(key))
    }

    override fun setgt(keys: List<String>): Boolean {
        lastNativeMethod = NativeMethod.setgt
        logEvent(LoggingKey.native_access_method, "Executing setgt on keys $keys")

        adapter.setPositioning(PositioningMethod.SETGT, keys)
        return true

    }

    override fun chain(key: String): Result {
        return chain(mutableListOf(key))
    }

    override fun chain(keys: List<String>): Result {
        lastNativeMethod = NativeMethod.chain
        logEvent(LoggingKey.native_access_method, "Executing chain on keys $keys")
        adapter.setRead(ReadMethod.CHAIN, keys)
        val read: Result
        measureTimeMillis {

            executeQuery(adapter.getSQLSatement())
            read = readNextFromResultSet()
        }.apply {
            logEvent(LoggingKey.native_access_method, "chain executed", this)
        }
        lastNativeMethod = null
        return read
    }

    override fun read(): Result {
        lastNativeMethod = NativeMethod.read
        logEvent(LoggingKey.native_access_method, "Executing read")
        val read: Result
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READ) ) {
                executeQuery(adapter.getSQLSatement())
            }
            read = readNextFromResultSet()
        }.apply {
            logEvent(LoggingKey.native_access_method, "read executed", this)
        }
        lastNativeMethod = null
        return read
    }

    override fun readPrevious(): Result {
        lastNativeMethod = NativeMethod.readPrevious
        logEvent(LoggingKey.native_access_method, "Executing readPrevious")
        val read: Result
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READP)) {
                executeQuery(adapter.getSQLSatement())
            }
            read = readNextFromResultSet()
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPrevious executed", this)
        }
        lastNativeMethod = null
        return read
    }

    override fun readEqual(): Result {
        return readEqual(adapter.getLastKeys())
    }

    override fun readEqual(key: String): Result {
        return readEqual(mutableListOf(key))
    }

    override fun readEqual(keys: List<String>): Result {
        lastNativeMethod = NativeMethod.readEqual
        logEvent(LoggingKey.native_access_method, "Executing readEqual on keys $keys")
        val read: Result
        measureTimeMillis {

            if (adapter.setRead(ReadMethod.READE, keys)) {
                executeQuery(adapter.getSQLSatement())
            }
            read = readNextFromResultSet()
        }.apply {
            logEvent(LoggingKey.native_access_method, "readEqual executed", this)
        }
        lastNativeMethod = null
        return read
    }


    override fun readPreviousEqual(): Result {
        return readPreviousEqual(adapter.getLastKeys())
    }

    override fun readPreviousEqual(key: String): Result {
        return readPreviousEqual(mutableListOf(key))
    }

    override fun readPreviousEqual(keys: List<String>): Result {
        lastNativeMethod = NativeMethod.readPreviousEqual
        logEvent(LoggingKey.native_access_method, "Executing readPreviousEqual on keys $keys")
        val read: Result
        measureTimeMillis {

            if (adapter.setRead(ReadMethod.READPE, keys)) {
                executeQuery(adapter.getSQLSatement())
            }
            read = readNextFromResultSet()
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPreviousEqual executed", this)
        }
        lastNativeMethod = null
        return read
    }

    override fun write(record: Record): Result {
        lastNativeMethod = NativeMethod.write
        logEvent(LoggingKey.native_access_method, "Executing write for record $record")
        measureTimeMillis {
            // TODO: manage errors
            val sql = fileMetadata.tableName.insertSQL(record)
            connection.prepareStatement(sql).use { it ->
                it.bind(record.values.map { it })
                it.execute()
            }
        }.apply {
            logEvent(LoggingKey.native_access_method, "write executed", this)
        }
        lastNativeMethod = null
        return Result(record)
    }

    override fun update(record: Record): Result {
        lastNativeMethod = NativeMethod.update
        logEvent(LoggingKey.native_access_method, "Executing update for record $record")
        measureTimeMillis {
            // record before update is "actualRecord"
            // record post update will be "record"
            var atLeastOneFieldChanged = false
            actualRecord?.forEach {
                val fieldValue = record.getValue(it.key)
                if(fieldValue != it.value){
                    atLeastOneFieldChanged = true
                    this.getResultSet()?.updateObject(it.key, fieldValue)
                }
            }
            if(atLeastOneFieldChanged){
                this.getResultSet()?.updateRow()
            }
        }.apply {
            logEvent(LoggingKey.native_access_method, "update executed", this)
        }
        lastNativeMethod = null
        return Result(record)
    }

    override fun delete(record: Record): Result {
        lastNativeMethod = NativeMethod.delete
        logEvent(LoggingKey.native_access_method, "Executing delete for record $record")
        measureTimeMillis {
            this.getResultSet()?.deleteRow()
        }.apply {
            logEvent(LoggingKey.native_access_method, "delete executed", this)
        }
        lastNativeMethod = null
        return Result(record)
    }

    private fun executeQuery(sqlAndValues: Pair<String, List<String>>) {
        executeQuery(sqlAndValues.first, sqlAndValues.second)
    }

    private fun executeQuery(sql: String, values: List<String>) {
        eof = false
        resultSet.closeIfOpen()
        logEvent(LoggingKey.execute_inquiry, "Preparing statement for query: $sql with bingings: $values")
        val stm: PreparedStatement
        measureTimeMillis {
            stm = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
            stm.bind(values)
        }.apply {
            logEvent(LoggingKey.execute_inquiry, "Statement prepared, executing query for statement", this)
        }
        measureTimeMillis {
            resultSet = stm.executeQuery()
        }.apply {
            logEvent(LoggingKey.execute_inquiry, "Query succesfully executed", this)
        }
    }


    private fun readNextFromResultSet(): Result {
        val result = Result(resultSet.toValues())
        if (!eof() && adapter.lastReadMatchRecord(result.record)) {
            logEvent(LoggingKey.read_data, "Record read: ${result.record}")
            actualRecord = result.record.duplicate()
            eof = false
            return result
        } else {
            eof = true
            closeResultSet()
            logEvent(LoggingKey.read_data, "No more record to read")
            return Result()
        }
    }

    private fun closeResultSet(){
        resultSet.closeIfOpen()
        resultSet = null
    }

    override fun eof() = eof


    override fun equal(): Boolean {
        logEvent(LoggingKey.read_data, "Read current record for equal")
        lastNativeMethod = NativeMethod.equal
        val result: Boolean
        if (!adapter.isLastOperationSet()) {
            result = false
        } else {

            measureTimeMillis {
                executeQuery(adapter.getReadSqlStatement())
                result = resultSet?.next() ?: false
            }.apply {
                logEvent(LoggingKey.read_data, "Record for equal read", this)
            }
        }
        lastNativeMethod = null
        return result
    }

    fun getResultSet(): ResultSet? {
        return this.resultSet
    }

    override fun close() {
        resultSet.closeIfOpen()
    }
}