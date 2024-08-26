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

class SQLDBFilePerf(
    override var name: String, override var fileMetadata: FileMetadata,
    var connection: Connection,
    override var logger: Logger? = null
) : DBFile {

    private var preparedStatements: MutableMap<String, PreparedStatement> = mutableMapOf()
    private var resultSet: ResultSet? = null
    private var actualRecord: Record? = null

    private var lastNativeMethod: NativeMethod? = null

    private var nextResult: Result? = null


    private var adapter: Native2SQLPerf = Native2SQLPerf(this.fileMetadata)
    private var eof: Boolean = false

    override fun setll(key: String): Boolean {
        return setll(mutableListOf(key))
    }

    override fun setll(keys: List<String>): Boolean {
        lastNativeMethod = NativeMethod.setll
        adapter.setPositioning(PositioningMethod.SETLL, keys)
        return true
    }

    override fun setgt(key: String): Boolean {
        return setgt(mutableListOf(key))
    }

    override fun setgt(keys: List<String>): Boolean {
        lastNativeMethod = NativeMethod.setgt
        adapter.setPositioning(PositioningMethod.SETGT, keys)
        return true

    }

    override fun chain(key: String): Result {
        return chain(mutableListOf(key))
    }

    override fun chain(keys: List<String>): Result {
        nextResult = null
        lastNativeMethod = NativeMethod.chain
        adapter.setRead(ReadMethod.CHAIN, keys)
        executeQuery(adapter.getSQLStatement())
        val read: Result = readNextFromResultSet(false)

        lastNativeMethod = null
        return read
    }

    override fun read(): Result {
        lastNativeMethod = NativeMethod.read
        var queryError = false
        if (adapter.setRead(ReadMethod.READ)) {
            try {
                executeQuery(adapter.getSQLStatement())
            } catch (e: Exception) {
                queryError = true
            }
        }
        val read: Result = readNextFromResultSet(true)
        read.indicatorLO = queryError

        lastNativeMethod = null
        return read
    }

    override fun readPrevious(): Result {
        lastNativeMethod = NativeMethod.readPrevious
        var queryError = false
        if (adapter.setRead(ReadMethod.READP)) {
            try {
                executeQuery(adapter.getSQLStatement())
            } catch (e: Exception) {
                queryError = true
            }
        }
        val read: Result = readNextFromResultSet(true)
        read.indicatorLO = queryError

        lastNativeMethod = null
        return read
    }

    override fun readEqual(): Result {
        var result = Result()
        try {
            result = readEqual(adapter.getLastKeys())
        } catch (exc: Exception) {
            result.indicatorLO = true
        }
        return result
    }

    override fun readEqual(key: String): Result {
        return readEqual(mutableListOf(key))
    }

    override fun readEqual(keys: List<String>): Result {
        lastNativeMethod = NativeMethod.readEqual
        var queryError = false
        if (adapter.setRead(ReadMethod.READE, keys)) {
            try {
                executeQuery(adapter.getSQLStatement())
            } catch (e: Exception) {
                queryError = true
            }
        }
        val read: Result = readNextFromResultSet(true)
        read.indicatorLO = queryError

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
        var queryError = false
        if (adapter.setRead(ReadMethod.READPE, keys)) {
            try {
                executeQuery(adapter.getSQLStatement())
            } catch (e: Exception) {
                queryError = true
            }
        }
        val read: Result = readNextFromResultSet(true)
        read.indicatorLO = queryError

        lastNativeMethod = null
        return read
    }

    override fun write(record: Record): Result {
        lastNativeMethod = NativeMethod.write
        // TODO: manage errors
        val sql = fileMetadata.tableName.insertSQL(record)
        connection.prepareStatement(sql).use { it ->
            it.bind(record.values.map { it })
            it.execute()
        }

        lastNativeMethod = null
        return Result(record)
    }

    override fun update(record: Record): Result {
        require(getResultSet() != null) {
            "Positioning required before update "
        }
        lastNativeMethod = NativeMethod.update
        // record before update is "actualRecord"
        // record post update will be "record"
        var atLeastOneFieldChanged = false
        actualRecord?.forEach {
            val fieldValue = record.getValue(it.key)
            if (fieldValue != it.value) {
                atLeastOneFieldChanged = true
                this.getResultSet()?.updateObject(it.key, fieldValue)
            }
        }
        if (atLeastOneFieldChanged) {
            this.getResultSet()?.updateRow()
        }

        lastNativeMethod = null
        return Result(record)
    }


    override fun delete(record: Record): Result {
        lastNativeMethod = NativeMethod.delete

        if (actualRecord != null) {
            this.getResultSet()?.deleteRow()
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
        val stm: PreparedStatement = preparedStatements.getOrPut(sql) {   connection.prepareStatement(
            sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE
        )}
        stm.bind(values)
        resultSet = stm.executeQuery()

    }


    private fun readNextFromResultSet(loadNext: Boolean): Result {
        if (nextResult?.record.isNullOrEmpty()) {
            nextResult = Result(resultSet.toValues())
        }

        val result = nextResult
        var found = false

        while (!found && !eof) {
            if (adapter.lastReadMatchRecord(result!!.record)) {
                actualRecord = result.record.duplicate()
                eof = false
                found = true
            }

            if (loadNext) {
                nextResult = Result(resultSet.toValues());

                if (nextResult!!.record.isEmpty()) {
                    eof = true
                    result.indicatorEQ = true
                    closeResultSet()
                }
            } else {
                eof = true
            }
        }
        return result!!
    }

    private fun closeResultSet() {
        resultSet.closeIfOpen()
        resultSet = null
    }

    override fun eof() = eof


    override fun equal(): Boolean {
        lastNativeMethod = NativeMethod.equal
        return if (adapter.isLastOperationSet()) {
            executeQuery(adapter.getReadSqlStatement())
            resultSet?.next() ?: false
        } else false
    }

    private fun getResultSet(): ResultSet? {
        return this.resultSet
    }

    override fun close() {
        resultSet.closeIfOpen()
        preparedStatements.values.forEach { it.close() }
    }
}