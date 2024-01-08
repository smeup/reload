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
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import kotlin.system.measureTimeMillis

class SQLDBFile(
    override var name: String, override var fileMetadata: FileMetadata,
    var connection: Connection,
    var jedis: Jedis,
    override var logger: Logger? = null
) : DBFile {

    constructor(
        name: String,
        fileMetadata: FileMetadata,
        connection: Connection
    ) : this(
        name,
        fileMetadata,
        connection,
        Jedis(System.getenv("REDIS_HOST"), Integer.parseInt(System.getenv("REDIS_PORT"))),
        null
    )

    constructor(
        name: String,
        fileMetadata: FileMetadata,
        connection: Connection,
        logger: Logger?
    ) : this(
        name,
        fileMetadata,
        connection,
        Jedis(System.getenv("REDIS_HOST"), Integer.parseInt(System.getenv("REDIS_PORT"))),
        logger
    )

    private var preparedStatements: MutableMap<String, PreparedStatement> = mutableMapOf()
    private var resultSet: ResultSet? = null
    private var actualRecord: Record? = null

    private var lastNativeMethod: NativeMethod? = null

    private var nextResult: Result? = null

    private var redisLoaded = false
    private var redisKeys: MutableList<String> = mutableListOf()

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

    private fun loadInRedis() {
        executeQuery(adapter.getGenericSQL(), mutableListOf())
        var cont = 0.0
        while (resultSet!!.next()) {
            val metaData: ResultSetMetaData = resultSet!!.metaData
            val columnCount: Int = metaData.columnCount

            val recordMap = mutableMapOf<String, Any>()
            var redisKey = ""
            for (i in 1..columnCount) {
                val columnName: String = metaData.getColumnName(i)
                val columnValue: Any = resultSet!!.getObject(i)

                if (columnName in fileMetadata.fileKeys) {
                    redisKey += columnValue.toString() + "_"
                }
                recordMap[columnName] = columnValue
            }
            val jsonRecord = recordMap.toString()
            jedis.setnx(redisKey.removeSuffix("_"), jsonRecord)
            jedis.zadd(fileMetadata.tableName, cont, redisKey.removeSuffix("_"))

            cont++
        }
        redisLoaded = true
    }

    private fun loadInRediswithKeys(keys: List<String>) {
        executeQuery(adapter.getGenericSQL(), mutableListOf())
        val fileKeys = fileMetadata.fileKeys.take(keys.size)
        //var cont = 0.0
        while (resultSet!!.next()) {
            val metaData: ResultSetMetaData = resultSet!!.metaData
            val columnCount: Int = metaData.columnCount

            val recordMap = mutableMapOf<String, Any>()
            var redisKey = "${fileMetadata.tableName}_"
            for (i in 1..columnCount) {
                val columnName: String = metaData.getColumnName(i)
                val columnValue: Any = resultSet!!.getObject(i)

                if (columnName in fileKeys) {
                    redisKey += columnValue.toString() + "_"
                }
                recordMap[columnName] = columnValue
            }
            //if(redisKey.removeSuffix("_").equals(fileKeys.joinToString("_"))){
            val jsonRecord = recordMap.toString()
            jedis.setnx(redisKey.removeSuffix("_"), jsonRecord)
            //}

            //jedis.zadd(fileMetadata.tableName, cont, redisKey.removeSuffix("_"))

            //cont++
        }
        redisLoaded = true
    }

    private fun retrieveFromRedis(keys: List<String>): Record {
        if (redisKeys.isEmpty())
            redisKeys = jedis.zrange(fileMetadata.tableName, 0, -1)
        val record = Record()
        for (key in redisKeys) {
            if (key.contains(keys.joinToString("_"))) {
                val value = jedis.get(key)
                val regex = Regex("""(\w+)=(.*?)(?:,|\})""")
                val matchResults = regex.findAll(value)
                for (matchResult in matchResults) {
                    val (key, value) = matchResult.destructured
                    record.add(RecordField(key, value))
                }
                return record
            }
        }
        return record
    }

    private fun retrieveFromRedis2(keys: List<String>): Record {
        val record = Record()
        var value = jedis.get(fileMetadata.tableName + "_" + keys.joinToString("_"))

        if(value != null){
            value = value.substring(1, value.length - 1)
            val keyValuePairs = value.split(',')
                .map { it }
                .mapNotNull {
                    if(!it.contains('=')) {
                        null
                    }else {
                        val (key, value) = it.split('=')
                        Pair(key.trim(), value)
                    }
                }
            for(pair in keyValuePairs){
                record.add(RecordField(pair.first, pair.second))
            }
        }
        return record
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
        nextResult = null
        lastNativeMethod = NativeMethod.chain
        logEvent(LoggingKey.native_access_method, "Executing chain on keys $keys")
        adapter.setRead(ReadMethod.CHAIN, keys)
        val read: Result
        measureTimeMillis {
            //executeQuery(adapter.getSQLSatement())
            if (!redisLoaded) {
                if (!isRedisLoaded(fileMetadata.tableName)) {
                    measureTimeMillis {
                        //loadInRedis()
                        loadInRediswithKeys(keys)
                    }.apply {
                        logEvent(LoggingKey.native_access_method, "Writing record to Redis", this)
                    }

                }
            }

            //read = Result(retrieveFromRedis(keys)) //readNextFromResultSet(false)
            read = Result(retrieveFromRedis2(keys))
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
        var queryError = false
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READ)) {
                try {
                    executeQuery(adapter.getSQLSatement())
                } catch (e: Exception) {
                    queryError = true
                    logEvent(LoggingKey.native_access_method, "Query execution failed: " + e.message)
                }
            }
            read = readNextFromResultSet(true)
            read.indicatorLO = queryError
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
        var queryError = false
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READP)) {
                try {
                    executeQuery(adapter.getSQLSatement())
                } catch (e: Exception) {
                    queryError = true
                    logEvent(LoggingKey.native_access_method, "Query execution failed: " + e.message)
                }
            }
            read = readNextFromResultSet(true)
            read.indicatorLO = queryError
        }.apply {
            logEvent(LoggingKey.native_access_method, "readPrevious executed", this)
        }
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
        logEvent(LoggingKey.native_access_method, "Executing readEqual on keys $keys")
        val read: Result
        var queryError = false
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READE, keys)) {
                try {
                    executeQuery(adapter.getSQLSatement())
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
        logEvent(LoggingKey.native_access_method, "Executing readPreviousEqual on keys $keys")
        val read: Result
        var queryError = false
        measureTimeMillis {
            if (adapter.setRead(ReadMethod.READPE, keys)) {
                try {
                    executeQuery(adapter.getSQLSatement())
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
        return read
    }

    override fun write(record: Record): Result {
        lastNativeMethod = NativeMethod.write
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
        return Result(record)
    }

    override fun update(record: Record): Result {
        require(getResultSet() != null) {
            "Positioning required before update "
        }
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
        return Result(record)
    }


    override fun delete(record: Record): Result {
        lastNativeMethod = NativeMethod.delete
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
            stm = preparedStatements.get(sql) ?: connection.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE
            )
            preparedStatements.putIfAbsent(sql, stm);
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


    private fun readNextFromResultSet(loadNext: Boolean): Result {
        if (nextResult == null || nextResult!!.record.isEmpty()) nextResult = Result(resultSet.toValues())
        val result = nextResult

        var found: Boolean = false
        while (!found && !eof) {
            if (adapter.lastReadMatchRecord(result!!.record)) {
                logEvent(LoggingKey.read_data, "Record read: ${result.record}")
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
                    logEvent(LoggingKey.read_data, "No more record to read")
                }
            } else {
                eof = true;
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
        jedis.close()
        resultSet.closeIfOpen()
        preparedStatements.values.forEach { it.close() }

        val prefix = fileMetadata.tableName
        val scanParams = ScanParams().match("$prefix*")
        var cursor = "0"

        do {
            val scanResult: ScanResult<String> = jedis.scan(cursor, scanParams)
            val keys = scanResult.result

            // Delete each key
            keys.forEach { key -> jedis.del(key) }

            // Update the cursor for the next iteration
            cursor = scanResult.cursor
        } while (cursor != "0")

        jedis.close()
    }

    private fun isRedisLoaded(tableName: String): Boolean {
        if (jedis.keys("$tableName*").isNotEmpty()) {
            redisLoaded = true
            jedis.close()
            return true
        } else {
            jedis.close()
            return false
        }
    }
}