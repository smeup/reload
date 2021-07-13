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
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingKey
import com.smeup.dbnative.log.NativeMethod
import com.smeup.dbnative.model.FileMetadata
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.system.measureTimeMillis

class SQLDBFileNoPerf(
                      override var fileMetadata: FileMetadata,
                      var connection: Connection,
                      override var logger: Logger? = null) : DBFile {

    constructor(
        name: String,
        fileMetadata: FileMetadata,
        connection: Connection): this(fileMetadata, connection, null)

    private var resultSet: ResultSet? = null
    private var movingForward = true
    private var lastKeys: List<RecordField> = emptyList()
    private var actualRecord: Record? = null
    private var actualRecordToPop: Record? = null
    private var eof: Boolean = false
    private var lastOperationSet: Boolean = false
    private var lastNativeMethod: NativeMethod? = null;



    private val thisFileKeys: List<String> by lazy {
        // TODO: think about a right way (local file maybe?) to retrieve keylist
        var indexes = this.fileMetadata.fileKeys
        if(indexes.isEmpty()){
            indexes = connection.primaryKeys(fileMetadata.name)
        }
        if (indexes.isEmpty()) connection.orderingFields(fileMetadata.name) else indexes
    }

    private fun logEvent(loggingKey: LoggingKey, message: String, elapsedTime: Long? = null) =
        logger?.logEvent(loggingKey, message, elapsedTime, lastNativeMethod, fileMetadata.tableName)

    override fun setll(key: String): Boolean {
        return setll(mutableListOf(key))
    }

    override fun setll(keys: List<String>): Boolean {
        lastNativeMethod = NativeMethod.setll
        logEvent(LoggingKey.native_access_method, "Executing setll on keys $keys")
        val point: Boolean
        measureTimeMillis {
            lastOperationSet = true

            var keyAsRecordField = keys.mapIndexed { index, value ->
                val keyname = thisFileKeys.get(index)
                RecordField(keyname, value)
            }

            checkAndStoreLastKeys(keyAsRecordField)
            movingForward = true
            point = point(keyAsRecordField);
        }.apply {
            logEvent(LoggingKey.native_access_method, "setll executed", this)
        }
        lastNativeMethod = null
        return point
    }

    override fun setgt(key: String): Boolean {

        return setgt(mutableListOf(key))
    }

    override fun setgt(keys: List<String>): Boolean {
        lastNativeMethod = NativeMethod.setgt
        logEvent(LoggingKey.native_access_method, "Executing setgt on keys $keys")
        val point: Boolean
        measureTimeMillis {
            lastOperationSet = true

            var keyAsRecordField = keys.mapIndexed { index, value ->
                val keyname = thisFileKeys.get(index)
                RecordField(keyname, value)
            }

            checkAndStoreLastKeys(keyAsRecordField)
            movingForward = false
            point = point(keyAsRecordField)
        }.apply {
            logEvent(LoggingKey.native_access_method, "setgt executed", this)
        }
        lastNativeMethod = null
        return point
    }

    override fun chain(key: String): Result {
        return chain(mutableListOf(key))
    }

    override fun chain(keys: List<String>): Result {
        lastNativeMethod = NativeMethod.chain
        logEvent(LoggingKey.native_access_method, "Executing chain on keys $keys")
        val read: Result
        measureTimeMillis {
            lastOperationSet = false

            var keyAsRecordField = keys.mapIndexed { index, value ->
                val keyname = thisFileKeys.get(index)
                RecordField(keyname, value)
            }

            checkAndStoreLastKeys(keyAsRecordField)
            movingForward = true
            calculateResultSet(keyAsRecordField)
            read = readFromResultSetFilteringBy(keyAsRecordField)
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
            lastOperationSet = false


            if (resultSet == null) {
                pointAtUpperLL()
            }
            if (!movingForward) {
                movingForward = true
                recalculateResultSet()
            }
            read = readFromResultSet()
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
            lastOperationSet = false

            if (resultSet == null) {
                pointAtUpperLL()
            }
            if (movingForward) {
                movingForward = false
                recalculateResultSet()
            }
            read = readFromResultSet()
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPrevious executed", this)
        }
        lastNativeMethod = null
        return read
    }

    override fun readEqual(): Result {
        val lastKeysAsList = lastKeys.map {
            it.value
        }
        return readEqual(lastKeysAsList)
    }

    override fun readEqual(key: String): Result {


        return readEqual(mutableListOf(key))
    }

    override fun readEqual(keys: List<String>): Result {
        lastNativeMethod = NativeMethod.readEqual
        logEvent(LoggingKey.native_access_method, "Executing readEqual")
        val read: Result
        measureTimeMillis {
            lastOperationSet = false

            var keysAsRecordField = keys.mapIndexed { index, value ->
                val keyname = thisFileKeys.get(index)
                RecordField(keyname, value)
            }

            checkAndStoreLastKeys(keysAsRecordField)
            if (resultSet == null) {
                pointAtUpperLL()
            }
            if (!movingForward) {
                movingForward = true
                recalculateResultSet()
            }
            read = readFromResultSetFilteringBy(keysAsRecordField)
        }.apply {
            logEvent(LoggingKey.native_access_method, "readEqual executed", this)
        }
        lastNativeMethod = null
        return read
    }

    override fun readPreviousEqual(): Result {

        val lastKeysAsList = lastKeys.map {
            it.value
        }

        return readPreviousEqual(lastKeysAsList)
    }

    override fun readPreviousEqual(key: String): Result {
        return readPreviousEqual(mutableListOf(key))
    }

    override fun readPreviousEqual(keys: List<String>): Result {
        lastNativeMethod = NativeMethod.readPreviousEqual
        logEvent(LoggingKey.native_access_method, "Executing readPreviousEqual on keys ${keys}")
        val read: Result
        measureTimeMillis {
            lastOperationSet = false

            var keyAsRecordField = keys.mapIndexed { index, value ->
                val keyname = thisFileKeys.get(index)
                RecordField(keyname, value)
            }

            checkAndStoreLastKeys(keyAsRecordField)
            if (resultSet == null) {
                pointAtUpperLL()
            }
            if (movingForward) {
                movingForward = false
                recalculateResultSet()
            }
            if (keys.isNotEmpty()) {
                lastKeys = keyAsRecordField
            }
            read = readFromResultSetFilteringBy(keyAsRecordField)
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPreviousEqual executed", this)
        }
        lastNativeMethod = null
        return read
    }

    override fun write(record: Record): Result {
        lastNativeMethod = NativeMethod.write
        logEvent(LoggingKey.native_access_method, "Executing write for record ${record}")
        lastOperationSet = false
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
        logEvent(LoggingKey.native_access_method, "Executing update for record ${record}")
        lastOperationSet = false
        measureTimeMillis {
            // record before update is "actualRecord"
            // record post update will be "record"
            var atLeastOneFieldChanged = false
            actualRecord?.forEach {
                var fieldValue = record.getValue(it.key)
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
        logEvent(LoggingKey.native_access_method, "Executing delete for record ${record}")
        lastOperationSet = false
        measureTimeMillis {
            this.getResultSet()?.deleteRow()
        }.apply {
            logEvent(LoggingKey.native_access_method, "delete executed", this)
        }
        lastNativeMethod = null
        return Result(record)
    }


    private fun executeQuery(sql: String, values: List<String>) {
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

    private fun checkAndStoreLastKeys(keys: List<RecordField>) {
        require(keys.isNotEmpty()) {
            "Missing keys"
        }
        lastKeys = keys
    }

    private fun pointAtUpperLL() {
        val sql = "SELECT * FROM ${fileMetadata.tableName} ${orderBySQL(thisFileKeys)}"
        executeQuery(sql, emptyList())
        readFromResultSet()
        actualRecordToPop = actualRecord
    }

    private fun point(keys: List<RecordField>): Boolean {
        calculateResultSet(keys)
        readFromResultSet()
        actualRecordToPop = actualRecord
        return resultSet.hasRecords()
    }

    // calculate the upper or the lower part of the ordered table given the input keys using an sql query (composed of selects in union if primary keys size > 1)
    private fun calculateResultSet(keys: List<RecordField>, withEquals: Boolean = true) {
        actualRecordToPop = null
        val sqlAndValues = filePartSQLAndValues(fileMetadata.tableName, movingForward, thisFileKeys, keys, withEquals)
        val values = sqlAndValues.first
        val sql = sqlAndValues.second
        executeQuery(sql, values)
    }

    private fun recalculateResultSet() {
        calculateResultSet(calculateRecordKeys(actualRecord, thisFileKeys), false)
    }

    private fun calculateRecordKeys(record: Record?, keysNames: List<String>): List<RecordField> {
        val result = mutableListOf<RecordField>()
        keysNames.forEach {
            result.add(RecordField(it, record!![it].toString()))
        }
        return result
    }

    private fun readFromResultSet(): Result {
        logEvent(LoggingKey.read_data, "Read record from ResultSet")
        val result: Result
        val record = Record()
        measureTimeMillis {
            if (actualRecordToPop != null) {
                result = Result(actualRecordToPop!!)
                actualRecordToPop = null
            } else {
                result = Result(resultSet.toValues())
            }
            record.putAll(result.record)
            actualRecord = record
        }.apply {
            logEvent(LoggingKey.read_data, "Record read $record", this)
        }
        return result
    }

    private fun readFromResultSetFilteringBy(keys: List<RecordField>): Result {
        logEvent(LoggingKey.search_data, "Searching record for keys: ${keys}")
        var result: Result
        var counter = 0
        measureTimeMillis {
            do {
                result = readFromResultSet()
                counter++
            } while (!result.record.matches(keys) && resultSet.hasRecords() && !eof())
        }.apply {
            logEvent(LoggingKey.search_data, "Search stops after $counter ResultSet iterations. Is eof: ${eof()}. Current row number is ${resultSet?.row?:" undefined"}}", this)
        }
        return result
    }

    private fun signalEOF() {
        resultSet?.last()
        resultSet?.next()
    }

    override fun eof(): Boolean = resultSet?.isAfterLast ?: true


    override fun equal(): Boolean {
        lastNativeMethod = NativeMethod.equal
        if (lastOperationSet == false) {
            return false
        } else {
            if (getResultSet() != null) {
                logEvent(LoggingKey.read_data, "Read current record for equal")
                val result: Boolean
                measureTimeMillis {
                    result = getResultSet().toValues().matches(lastKeys)
                    getResultSet()?.previous()
                }.apply {
                    logEvent(LoggingKey.read_data, "Record for equal read", this)
                }
                return result
            } else {
                return false
            }
        }
        lastNativeMethod = null
    }

    fun getResultSet(): ResultSet? {
        return this.resultSet
    }
}