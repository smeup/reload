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
import com.smeup.dbnative.model.FileMetadata
import java.sql.Connection
import java.sql.ResultSet

class SQLDBFile(override var name: String,
                override var fileMetadata: FileMetadata,
                var connection: Connection,
                override var logger: Logger? = null) : DBFile {

    private var resultSet: ResultSet? = null
    private var movingForward = true
    private var lastKeys: List<RecordField> = emptyList()
    private var actualRecord: Record? = null
    private var actualRecordToPop: Record? = null
    private var eof: Boolean = false
    private var lastOperationSet: Boolean = false

    private val thisFileKeys: List<String> by lazy {
        // TODO: think about a right way (local file maybe?) to retrieve keylist
        var indexes = this.fileMetadata.fileKeys
        if(indexes.isEmpty()){
            indexes = connection.primaryKeys(name)
        }
        if (indexes.isEmpty()) connection.orderingFields(name) else indexes
    }


    override fun setll(key: String): Boolean {
        return setll(mutableListOf(key))
    }

    override fun setll(keys: List<String>): Boolean {
        logger?.logEvent(LoggingKey.native_access_method, "Executing setll on keys $keys")
        lastOperationSet = true

        var keyAsRecordField = keys.mapIndexed { index, value ->
            val keyname = thisFileKeys.get(index)
            RecordField(keyname, value)
        }

        checkAndStoreLastKeys(keyAsRecordField)
        movingForward = true
        return point(keyAsRecordField).apply { logger?.logEvent(LoggingKey.native_access_method, "setll executed") }
    }

    override fun setgt(key: String): Boolean {

        return setgt(mutableListOf(key))
    }

    override fun setgt(keys: List<String>): Boolean {
        logger?.logEvent(LoggingKey.native_access_method, "Executing setgt on keys $keys")
        lastOperationSet = true

        var keyAsRecordField = keys.mapIndexed { index, value ->
            val keyname = thisFileKeys.get(index)
            RecordField(keyname, value)
        }

        checkAndStoreLastKeys(keyAsRecordField)
        movingForward = false
        return point(keyAsRecordField).apply { logger?.logEvent(LoggingKey.native_access_method, "setgt executed") }
    }

    override fun chain(key: String): Result {
        return chain(mutableListOf(key))
    }

    override fun chain(keys: List<String>): Result {
        logger?.logEvent(LoggingKey.native_access_method, "Executing chain on keys $keys")
        lastOperationSet = false

        var keyAsRecordField = keys.mapIndexed { index, value ->
            val keyname = thisFileKeys.get(index)
            RecordField(keyname, value)
        }

        checkAndStoreLastKeys(keyAsRecordField)
        movingForward = true
        calculateResultSet(keyAsRecordField)
        return readFromResultSetFilteringBy(keyAsRecordField).apply { logger?.logEvent(LoggingKey.native_access_method, "chain executed") }
    }

    override fun read(): Result {
        logger?.logEvent(LoggingKey.native_access_method, "Executing read")
        lastOperationSet = false


        if (resultSet == null) {
            pointAtUpperLL()
        }
        if (!movingForward) {
            movingForward = true
            recalculateResultSet()
        }
        return readFromResultSet().apply { logger?.logEvent(LoggingKey.native_access_method, "read executed") }
    }

    override fun readPrevious(): Result {
        logger?.logEvent(LoggingKey.native_access_method, "Executing readPrevious")
        lastOperationSet = false

        if (resultSet == null) {
            pointAtUpperLL()
        }
        if (movingForward) {
            movingForward = false
            recalculateResultSet()
        }
        return readFromResultSet().apply { logger?.logEvent(LoggingKey.native_access_method, "readPrevious executed") }
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
        logger?.logEvent(LoggingKey.native_access_method, "Executing readEqual")
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
        return readFromResultSetFilteringBy(keysAsRecordField).apply { logger?.logEvent(LoggingKey.native_access_method, "readEqual executed") }
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
        logger?.logEvent(LoggingKey.native_access_method, "Executing readPreviousEqual on keys ${keys}")

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
        return readFromResultSetFilteringBy(keyAsRecordField).apply { logger?.logEvent(LoggingKey.native_access_method, "readPreviousEqual executed") }
    }

    override fun write(record: Record): Result {
        logger?.logEvent(LoggingKey.native_access_method, "Executing write for record ${record}")
        lastOperationSet = false

        // TODO: manage errors
        val sql = name.insertSQL(record)
        connection.prepareStatement(sql).use { it ->
            it.bind(record.values.map { it })
            it.execute()
        }
        return Result(record).apply { logger?.logEvent(LoggingKey.native_access_method, "write executed") }
    }

    override fun update(record: Record): Result {
        logger?.logEvent(LoggingKey.native_access_method, "Executing update for record ${record}")
        lastOperationSet = false

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
        return Result(record).apply { logger?.logEvent(LoggingKey.native_access_method, "update executed") }
    }

    override fun delete(record: Record): Result {
        logger?.logEvent(LoggingKey.native_access_method, "Executing delete for record ${record}")
        lastOperationSet = false

        this.getResultSet()?.deleteRow()
        return Result(record).apply { logger?.logEvent(LoggingKey.native_access_method, "delete executed") }
    }


    private fun executeQuery(sql: String, values: List<String>) {
        resultSet.closeIfOpen()
        logger?.logEvent(LoggingKey.execute_inquiry, "Preparing statement for query: $sql with bingings: $values")
        val stm = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
        stm.bind(values)
        logger?.logEvent(LoggingKey.execute_inquiry, "Statement prepared, executing query for statement")
        resultSet = stm.executeQuery().apply { logger?.logEvent(LoggingKey.execute_inquiry, "Query succesfully xecuted") }
    }

    private fun checkAndStoreLastKeys(keys: List<RecordField>) {
        require(keys.isNotEmpty()) {
            "Missing keys"
        }
        lastKeys = keys
    }

    private fun pointAtUpperLL() {
        val sql = "SELECT * FROM $name ${orderBySQL(thisFileKeys)}"
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
        val sqlAndValues = filePartSQLAndValues(name, movingForward, thisFileKeys, keys, withEquals)
        val values = sqlAndValues.first
        val sql = sqlAndValues.second
        executeQuery(sql, values)
    }

    // NOTE: unused impl left for hint
    // created a calculated key called NATIVE_ACCESS_MARKER, that gives to records an order based on all the primary keys of the table and used it to calculate
    // the upper or the lower part of the ordered table given the input keys
    private fun calculateResultSetWithMarker(keys: List<RecordField>, withEquals: Boolean = true) {
        actualRecordToPop = null
        // NOTE: use the key field if primary keys size == 1
        // NOTE: NATIVE_ACCESS_MARKER can be avoided if you create and index a unique field key concordant with all the primary keys
        // NOTE: if using NATIVE_ACCESS_MARKER be careful with length (primary key fields must be of fixed length) and with dates, numbers or not string formats -> transform them into key strings
        val sql =
            "SELECT * FROM (SELECT $name.*, ${createMarkerSQL(thisFileKeys)} FROM $name) AS NATIVE_ACCESS_WT ${markerWhereSQL(
                movingForward, withEquals
            )} ${orderBySQL(
                thisFileKeys,
                reverse = !movingForward
            )}"
        val values = listOf(calculateMarkerValue(keys, movingForward, withEquals))
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
        logger?.logEvent(LoggingKey.read_data, "Read record from ResultSet")
        val result: Result
        if (actualRecordToPop != null) {
            result = Result(actualRecordToPop!!)
            actualRecordToPop = null
        } else {
            result = Result(resultSet.toValues())
        }
        val record: Record = Record()
        record.putAll(result.record)
        actualRecord = record
        logger?.logEvent(LoggingKey.read_data, "Record read $record")
        return result
    }

    private fun readFromResultSetFilteringBy(keys: List<RecordField>): Result {
        logger?.logEvent(LoggingKey.search_data, "Searching record for keys: ${keys}")
        var result: Result
        var counter = 0
        do {
            result = readFromResultSet()
            counter++
        } while (!result.record.matches(keys) && resultSet.hasRecords() && !eof())
        logger?.logEvent(LoggingKey.search_data, "Search stops after $counter ResultSet iterations. Is eof: ${eof()}. Current row number is ${resultSet?.row?:" undefined"}}")
        return result
    }

    private fun signalEOF() {
        resultSet?.last()
        resultSet?.next()
    }

    override fun eof(): Boolean = resultSet?.isAfterLast ?: true


    override fun equal(): Boolean {
        if (lastOperationSet == false) {
            return false
        } else {
            if (getResultSet() != null) {
                logger?.logEvent(LoggingKey.read_data, "Read current record for equal")
                val result = getResultSet().toValues().matches(lastKeys)
                getResultSet()?.previous()
                logger?.logEvent(LoggingKey.read_data, "Record for equal read")
                return result
            } else {
                return false
            }
        }
    }

    fun getResultSet(): ResultSet? {
        return this.resultSet
    }
}