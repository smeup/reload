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
import com.smeup.dbnative.log.TelemetrySpan
import com.smeup.dbnative.model.FileMetadata
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.system.measureTimeMillis

class SQLDBFile(
    override var name: String, override var fileMetadata: FileMetadata,
    var connection: Connection,
    override var logger: Logger? = null
) : DBFile {

    private var preparedStatements: MutableMap<String, PreparedStatement> = mutableMapOf()
    private var resultSet: ResultSet? = null
    private var actualRecord: Record? = null

    private var lastNativeMethod: NativeMethod? = null

    //Search from: metadata, primary key, unique index, view ordering fields
    //private val thisFileKeys: List<String> by lazy {
    //    // TODO: think about a right way (local file maybe?) to retrieve keylist
    //    var indexes = this.fileMetadata.fileKeys
    //    if(indexes.isEmpty()){
    //        indexes = connection.primaryKeys(fileMetadata.name)
    //    }
    //    }
    //    if (indexes.isEmpty()) connection.orderingFields(fileMetadata.name) else indexes
    //}

    private var adapter: Native2SQL = Native2SQL(this.fileMetadata)
    private var eof: Boolean = false

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
        val telemetrySpan = TelemetrySpan("CHAIN Execution")
        lastNativeMethod = NativeMethod.chain
        logEvent(LoggingKey.native_access_method, "Executing chain on keys $keys")
        adapter.setRead(ReadMethod.CHAIN, keys)
        val read: Result
        measureTimeMillis {
            executeQuery(adapter.getSQLStatement())
            read = readNextFromResultSet(true)
        }.apply {
            logEvent(LoggingKey.native_access_method, "chain executed", this)
        }
        lastNativeMethod = null
        telemetrySpan.endSpan()
        return read
    }

    override fun read(): Result {
        val telemetrySpan = TelemetrySpan("READ Execution")
        lastNativeMethod = NativeMethod.read
        logEvent(LoggingKey.native_access_method, "Executing read")
        val read: Result
        var queryError = false
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READ)) {
                try {
                    executeQuery(adapter.getSQLStatement())
                } catch (e: Exception) {
                    queryError = true
                    logEvent(LoggingKey.native_access_method, "Query execution failed: " + e.message)
                }
            }
            read = readNextFromResultSet(false)
            read.indicatorLO = queryError
        }.apply {
            logEvent(LoggingKey.native_access_method, "read executed", this)
        }
        lastNativeMethod = null
        telemetrySpan.endSpan()
        return read
    }

    override fun readPrevious(): Result {
        val telemetrySpan = TelemetrySpan("READP Execution")
        lastNativeMethod = NativeMethod.readPrevious
        logEvent(LoggingKey.native_access_method, "Executing readPrevious")
        val read: Result
        var queryError = false
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READP)) {
                try {
                    executeQuery(adapter.getSQLStatement())
                } catch (e: Exception) {
                    queryError = true
                    logEvent(LoggingKey.native_access_method, "Query execution failed: " + e.message)
                }
            }
            read = readNextFromResultSet(false)
            read.indicatorLO = queryError
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPrevious executed", this)
        }
        lastNativeMethod = null
        telemetrySpan.endSpan()
        return read
    }

    override fun readEqual(): Result {
        val telemetrySpan = TelemetrySpan("READE Execution")
        var result = Result()
        try {
            result = readEqual(adapter.getLastKeys())
        } catch (exc: Exception) {
            result.indicatorLO = true
        }
        telemetrySpan.endSpan()
        return result
    }

    override fun readEqual(key: String): Result {
        return readEqual(mutableListOf(key))
    }

    override fun readEqual(keys: List<String>): Result {
        lastNativeMethod = NativeMethod.readEqual
        logEvent(LoggingKey.native_access_method, "Executing readEqual on keys $keys")
        val read: Result
        var queryError = false
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READE, keys)) {
                try {
                    executeQuery(adapter.getSQLStatement())
                } catch (e: Exception) {
                    queryError = true
                    logEvent(LoggingKey.native_access_method, "Query execution failed: " + e.message)
                }
            }
            read = readNextFromResultSet(true)
            read.indicatorLO = queryError
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
        val telemetrySpan = TelemetrySpan("READPE Execution")
        logEvent(LoggingKey.native_access_method, "Executing readPreviousEqual on keys $keys")
        val read: Result
        var queryError = false
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READPE, keys)) {
                try {
                    executeQuery(adapter.getSQLStatement())
                } catch (e: Exception) {
                    queryError = true
                    logEvent(LoggingKey.native_access_method, "Query execution failed: " + e.message)
                }
            }
            read = readNextFromResultSet(true)
            read.indicatorLO = queryError
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPreviousEqual executed", this)
        }
        lastNativeMethod = null
        telemetrySpan.endSpan()
        return read
    }

    override fun write(record: Record): Result {
        lastNativeMethod = NativeMethod.write
        val telemetrySpan = TelemetrySpan("WRITE Execution")
        logEvent(
            LoggingKey.native_access_method,
            "Executing write for record $record: with autocommit=${connection.autoCommit}"
        )
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
        telemetrySpan.endSpan()
        return Result(record)
    }

    /*
    override fun update(record: Record): Result {
        lastNativeMethod = NativeMethod.update
        logEvent(LoggingKey.native_access_method, "Executing write for record $record: with autocommit=${connection.autoCommit}")
        measureTimeMillis {
            // TODO: manage errors
            val sql = fileMetadata.tableName.updateSQL(record)
            connection.prepareStatement(sql).use { it ->
                it.bind(record.values.map { it })
                it.execute()
            }
        }.apply {
            logEvent(LoggingKey.native_access_method, "update executed", this)
        }
        lastNativeMethod = null
        return Result(record)
    }
    */

    override fun update(record: Record): Result {
        require(getResultSet() != null) {
            "Positioning required before update "
        }
        val telemetrySpan = TelemetrySpan("UPDATE Execution")
        lastNativeMethod = NativeMethod.update
        logEvent(
            LoggingKey.native_access_method,
            "Executing update record $actualRecord to $record with autocommit=${connection.autoCommit}"
        )
        measureTimeMillis {
            // record before update is "actualRecord"
            // record post update will be "record"
            var atLeastOneFieldChanged = false
            actualRecord?.forEach {
                val fieldValue = record.getValue(it.key)
                if (fieldValue != it.value) {
                    atLeastOneFieldChanged = true
                    this.getResultSet()?.updateObject(it.key, fieldValue)
                }
            } ?: logEvent(LoggingKey.native_access_method, "No previous read executed, nothing to update")
            if (atLeastOneFieldChanged) {
                this.getResultSet()?.updateRow()
            }
        }.apply {
            logEvent(LoggingKey.native_access_method, "update executed", this)
        }
        lastNativeMethod = null
        telemetrySpan.endSpan()
        return Result(record)
    }


    override fun delete(record: Record): Result {
        lastNativeMethod = NativeMethod.delete
        val telemetrySpan = TelemetrySpan("DELETE Execution")
        logEvent(
            LoggingKey.native_access_method,
            "Executing delete for current record $actualRecord with autocommit=${connection.autoCommit}"
        )
        measureTimeMillis {
            if (actualRecord != null) {
                this.getResultSet()?.deleteRow()
            } else {
                logEvent(LoggingKey.native_access_method, "No previous read executed, nothing to delete")
            }
        }.apply {
            logEvent(LoggingKey.native_access_method, "delete executed", this)
        }
        lastNativeMethod = null
        telemetrySpan.endSpan()
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
            stm = preparedStatements.getOrPut(sql) {
                connection.prepareStatement(
                    sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE
                )
            }
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


    private fun readNextFromResultSet(exitOnUnmatch: Boolean): Result {
        var found = false
        var result = Result(resultSet.toValues())
        var count = 0
        while (!found && !eof) {
            count++
            if (result.record.isEmpty()) {
                eof = true
                result.indicatorEQ = true
                closeResultSet()
                logEvent(LoggingKey.read_data, "No more record to read")
            }
            else if (adapter.lastReadMatchRecord(result.record)) {
                logEvent(LoggingKey.read_data, "Record read: ${result.record}")
                actualRecord = result.record.duplicate()
                eof = false
                found = true
            } else {
                logEvent(LoggingKey.read_data, "Readed records: ${count}")
                if (exitOnUnmatch) {
                    eof = true
                    found = false
                    result = Result()
                    result.indicatorEQ = true
                } else {
                    result = Result(resultSet.toValues())
                }
            }
        }
        return result
    }

    private fun closeResultSet() {
        resultSet.closeIfOpen()
        resultSet = null
    }

    override fun eof() = eof


    override fun equal(): Boolean {
        logEvent(LoggingKey.read_data, "Read current record for equal")
        lastNativeMethod = NativeMethod.equal
        val result: Boolean
        if (adapter.isLastOperationSet()) {
            measureTimeMillis {
                executeQuery(adapter.getReadSqlStatement())
                result = resultSet?.next() ?: false
            }.apply {
                logEvent(LoggingKey.read_data, "Record for equal read", this)
            }
        } else {
            result = false
        }
        lastNativeMethod = null
        return result
    }

    private fun getResultSet(): ResultSet? {
        return this.resultSet
    }

    override fun close() {
        resultSet.closeIfOpen()
        preparedStatements.values.forEach { it.close() }
    }
}