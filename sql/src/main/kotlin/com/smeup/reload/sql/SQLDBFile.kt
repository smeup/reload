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

package com.smeup.reload.sql

import com.smeup.reload.file.DBFile
import com.smeup.reload.file.Record
import com.smeup.reload.file.RecordField
import com.smeup.reload.file.Result
import com.smeup.reload.model.FileMetadata
import java.sql.Connection
import java.sql.ResultSet

class SQLDBFile(override var name: String, override var fileMetadata: FileMetadata, var connection: Connection) : DBFile {

    private var resultSet: ResultSet? = null
    private var movingForward = true
    private var lastKeys: List<RecordField> = emptyList()
    private var actualRecord: Record? = null
    private var actualRecordToPop: Record? = null

    private val thisFileKeys: List<String> by lazy {
        // TODO: think about a right way (local file maybe?) to retrieve keylist
        var indexes = this.fileMetadata.fileKeys
        if(indexes.isEmpty()){
            indexes = connection.primaryKeys(name)
        }
        if (indexes.isEmpty()) connection.orderingFields(name) else indexes
    }

    override fun setll(key: String): Boolean {
        return setll(toFields(key))
    }

    override fun setll(keys: List<RecordField>): Boolean {
        checkAndStoreLastKeys(keys)
        movingForward = true
        return point(keys)
    }

    override fun setgt(key: String): Boolean {
        return setgt(toFields(key))
    }

    override fun setgt(keys: List<RecordField>): Boolean {
        checkAndStoreLastKeys(keys)
        movingForward = false
        return point(keys)
    }

    override fun chain(key: String): Result {
        return chain(toFields(key))
    }

    override fun chain(keys: List<RecordField>): Result {
        checkAndStoreLastKeys(keys)
        movingForward = true
        calculateResultSet(keys)
        return readFromResultSetFilteringBy(keys)
    }

    override fun read(): Result {
        if (resultSet == null) {
            pointAtUpperLL()
        }
        if (!movingForward) {
            movingForward = true
            recalculateResultSet()
        }
        return readFromResultSet()
    }

    override fun readPrevious(): Result {
        if (resultSet == null) {
            pointAtUpperLL()
        }
        if (movingForward) {
            movingForward = false
            recalculateResultSet()
        }
        return readFromResultSet()
    }

    override fun readEqual(): Result {
        return readEqual(lastKeys)
    }

    override fun readEqual(key: String): Result {

        return readEqual(toFields(key))
    }

    override fun readEqual(keys: List<RecordField>): Result {
        checkAndStoreLastKeys(keys)
        if (resultSet == null) {
            pointAtUpperLL()
        }
        if (!movingForward) {
            movingForward = true
            recalculateResultSet()
        }
        return readFromResultSetFilteringBy(keys)
    }

    override fun readPreviousEqual(): Result {
        return readPreviousEqual(lastKeys)
    }

    override fun readPreviousEqual(key: String): Result {
        return readPreviousEqual(toFields(key))
    }

    override fun readPreviousEqual(keys: List<RecordField>): Result {
        checkAndStoreLastKeys(keys)
        if (resultSet == null) {
            pointAtUpperLL()
        }
        if (movingForward) {
            movingForward = false
            recalculateResultSet()
        }
        if (keys.isNotEmpty()) {
            lastKeys = keys
        }
        return readFromResultSetFilteringBy(keys)
    }

    override fun write(record: Record): Result {
        // TODO: manage errors
        val sql = name.insertSQL(record)
        connection.prepareStatement(sql).use { it ->
            it.bind(record.values.map { it })
            it.execute()
        }
        return Result(record)
    }

    override fun update(record: Record): Result {
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
        return Result(record)
    }

    override fun delete(record: Record): Result {
        this.getResultSet()?.deleteRow()
        return Result(record)
    }

    private fun toFields(keyValue: String): List<RecordField> {
        // TODO Not clear implementation, it needs investigation. I suppose that this method should be to return first field of table.
        val keyName = thisFileKeys.first()
        return listOf(RecordField(keyName, keyValue))
    }

    private fun executeQuery(sql: String, values: List<String>) {
        resultSet.closeIfOpen()
        val stm = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
        stm.bind(values)
        resultSet = stm.executeQuery()
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
        return result
    }

    private fun readFromResultSetFilteringBy(keys: List<RecordField>): Result {
        var result: Result
        do {
            result = readFromResultSet()
        } while (!result.record.matches(keys) && resultSet.hasRecords() && !eof())
        return result
    }

    private fun signalEOF() {
        resultSet?.last()
        resultSet?.next()
    }

    private fun eof(): Boolean = resultSet?.isAfterLast ?: true

    fun getResultSet(): ResultSet? {
        return this.resultSet
    }
}