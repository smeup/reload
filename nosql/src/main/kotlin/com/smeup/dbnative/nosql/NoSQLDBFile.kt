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

package com.smeup.dbnative.nosql

import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingKey
import com.smeup.dbnative.log.NativeMethod
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.nosql.utils.*
import com.smeup.dbnative.utils.toRecordFields
import org.bson.Document
import kotlin.system.measureTimeMillis

class NoSQLDBFile(override var name: String,
                  override var fileMetadata: FileMetadata,
                  private val database: MongoDatabase,
                  override var logger: Logger? = null): DBFile {

    private var globalCursor: MongoCursor<Document>? = null
    //private var last_set_keys: List<RecordField> = emptyList()
    private var actualRecord: Record? = null
    private var eof: Boolean = false
    private var lastNativeMethod: NativeMethod? = null
    private var adapter: Native2Mongo = Native2Mongo(this.fileMetadata.fileKeys, fileMetadata.tableName)

    private fun logEvent(loggingKey: LoggingKey, message: String, elapsedTime: Long? = null) =
        logger?.logEvent(loggingKey, message, elapsedTime, lastNativeMethod, fileMetadata.name)

    override fun eof(): Boolean {
        return eof
    }

    override fun equal(): Boolean {
        logEvent(LoggingKey.read_data, "Read current record for equal")
        lastNativeMethod = NativeMethod.equal
        val result: Boolean
        if (!adapter.isLastOperationSet()) {
            result = false
        } else {
            measureTimeMillis {
                executeQuery(adapter.getReadQuery())
                result = globalCursor?.next()?.let { return true } ?: false
            }.apply {
                logEvent(LoggingKey.read_data, "Record for equal read", this)
            }
        }
        lastNativeMethod = null
        return result
    }


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
        var result = Result(Record())
        measureTimeMillis {
            eof = true
            getFirstDocumentMatchingKey(fileMetadata.toRecordFields(keys))?.apply {
                val record = this.toRecord()
                result = Result(record)
                eof = false
            }

        }.apply {
            logEvent(LoggingKey.native_access_method, "chain executed, result: $result", this)
        }
        lastNativeMethod = null
        return result
    }

    override fun read(): Result {
        lastNativeMethod = NativeMethod.read
        logEvent(LoggingKey.native_access_method, "Executing read")
        val read: Result
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READ)) {
                executeQuery(adapter.getQuery())
            }
            read = readNextFromCursor()
        }.apply {
            logEvent(LoggingKey.native_access_method, "read executed", this)
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
                executeQuery(adapter.getQuery())
            }
            read = readNextFromCursor()
        }.apply {
            logEvent(LoggingKey.native_access_method, "readEqual executed", this)
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
                executeQuery(adapter.getQuery())
            }
            read = readNextFromCursor()
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPrevious executed", this)
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
                executeQuery(adapter.getQuery())
            }
            read = readNextFromCursor()
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPreviousEqual executed", this)
        }
        return read
    }


    override fun write(record: Record): Result {
        lastNativeMethod = NativeMethod.write
        logEvent(LoggingKey.native_access_method, "Executing write for record $record")
        val result: Result
        measureTimeMillis {
            val insertCommand = fileMetadata.buildInsertCommand(fileMetadata.tableName, record)
            logEvent(LoggingKey.execute_inquiry, "Executing insert command $insertCommand")
            executeCommand(insertCommand)
            result = Result(record = record)
        }.apply {
            logEvent(LoggingKey.native_access_method, "write executed", this)
        }
        lastNativeMethod = null
        return result
    }

    override fun update(record: Record): Result {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun delete(record: Record): Result {
        TODO("Not yet implemented")
    }

    private fun executeCommand(command: String) = database.runCommand(Document.parse(command))

    private fun getFirstDocumentMatchingKey(keys: List<RecordField>): Document? {
        var document: Document?
        measureTimeMillis {
            val filter = adapter.getQuery()
            logEvent(LoggingKey.execute_inquiry, "Built filter command ${filter.first}")
            document = database.getCollection(fileMetadata.tableName).find(Document.parse(filter.first))?.limit(1)?.first()
        }.apply {
            logEvent(LoggingKey.execute_inquiry, "Query built and executed", this)
        }
        return document
    }

    override fun close() {
        globalCursor?.close()
    }

    private fun executeQuery(filterAndSort: Pair<String, String>){
        executeQuery(filterAndSort.first, filterAndSort.second)
    }

    private fun executeQuery(filter: String, sort: String){
        logEvent(LoggingKey.execute_inquiry, "Executing query $filter with sort $sort")
        measureTimeMillis {
            var cursor = database.getCollection(fileMetadata.tableName).find(Document.parse(filter))
            cursor = if (sort.isNullOrEmpty()) cursor else cursor.sort(Document.parse(sort))
            globalCursor = cursor.iterator()
        }.apply {
            logEvent(LoggingKey.execute_inquiry, "Query succesfully executed", this)
        }
    }

    private fun readNextFromCursor(): Result{
        var result = Result()
        if (!eof()) {
            result = Result(globalCursor.toValues())
            if (adapter.lastReadMatchRecord(result.record)) {
                logEvent(LoggingKey.read_data, "Record read: ${result.record}")
                actualRecord = result.record.duplicate()
                eof = false
            } else {
                eof = true
                globalCursor?.close()
                logEvent(LoggingKey.read_data, "No more record to read")
            }
        }
        return result
    }


//    private fun calculateCursor(keys: List<RecordField>, up_direction: Boolean, includeFirst: Boolean): FindIterable<Document> {
//        var operator1: MongoOperator = MongoOperator.EQ
//        var operator2: MongoOperator = MongoOperator.EQ
//        var operator3: MongoOperator = MongoOperator.EQ
//
//        when {
//            up_direction && includeFirst -> {
//                operator1 = MongoOperator.GE
//                operator2 = MongoOperator.GT
//                operator3 = MongoOperator.GE
//            }
//
//            !up_direction && includeFirst -> {
//                operator1 = MongoOperator.LE
//                operator2 = MongoOperator.LT
//                operator3 = MongoOperator.LE
//            }
//
//            up_direction && !includeFirst -> {
//                operator1 = MongoOperator.GT
//                operator2 = MongoOperator.GT
//                operator3 = MongoOperator.GE
//            }
//
//            !up_direction && !includeFirst -> {
//                operator1 = MongoOperator.LT
//                operator2 = MongoOperator.LT
//                operator3 = MongoOperator.LE
//            }
//            else -> {}
//        }
//
//        val filter = StringBuilder()
//
//        filter.append("{\$or:[")
//
//        val orContent = StringBuilder()
//
//
//        // Add first line
//        val line = StringBuilder()
//
//        line.append("{\$and:[")
//
//        keys.forEachIndexed { index, recordField ->
//            if (index != keys.size-1) {
//                line.append("{ \"${recordField.name}\": {${operator3.symbol} \"${recordField.value}\" } }, ")
//            }
//            else{
//                line.append("{ \"${recordField.name}\": {${operator1.symbol} \"${recordField.value}\" } }, ")
//            }
//        }
//
//        line.append("] }")
//
//        orContent.append(line.toString())
//
//
//        if (keys.size > 1) {
//
//            var while_index = keys.size
//
//            while (while_index > 1) {
//
//                val tempLine = StringBuilder()
//
//                tempLine.append(", {\$and:[")
//
//                val subList = keys.subList(0, while_index-1)
//
//                subList.forEachIndexed{index: Int, recordField: RecordField ->
//                    if (index != subList.size-1) {
//                        tempLine.append("{ \"${recordField.name}\": {${operator3.symbol} \"${recordField.value}\" } }, ")
//                    }
//                    else{
//                        tempLine.append("{ \"${recordField.name}\": {${operator2.symbol} \"${recordField.value}\" } }, ")
//                    }
//                }
//
//                tempLine.append("] }")
//
//                orContent.append(tempLine.toString())
//
//                while_index--
//            }
//        }
//
//        filter.append(orContent.toString())
//
//        filter.append("] }")
//
//
//        val sort = StringBuilder()
//
//        keys.joinTo(sort, separator= ",", prefix= "{", postfix = "}") {
//            if (up_direction) {
//                "\"${it.name}\": 1"
//            } else {
//                "\"${it.name}\": -1"
//            }
//        }
//        sort.append("}")
//        logEvent(LoggingKey.execute_inquiry, "Built filter command $filter with sort $sort")
//        //println("$filter - $sort")
//
//        val cursor = database.getCollection(fileMetadata.tableName).find(Document.parse(filter.toString()))
//
//        return cursor.sort(Document.parse(sort.toString()))
//    }

//    private fun updateLastKeys(record: Record) {
//
//        val lastKeys = mutableListOf<RecordField>()
//        fileMetadata.fileKeys.forEach {
//            lastKeys.add(RecordField(it, record.getValue(it)))
//        }
//        last_set_keys = lastKeys
//    }
}

