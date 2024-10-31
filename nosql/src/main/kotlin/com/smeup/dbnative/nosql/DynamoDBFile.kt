package com.smeup.dbnative.nosql

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.document.*
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingKey
import com.smeup.dbnative.log.NativeMethod
import com.smeup.dbnative.log.TelemetrySpan
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.utils.matchFileKeys
import kotlin.system.measureTimeMillis

class DynamoDBFile(
    override var name: String,
    override var fileMetadata: FileMetadata,
    private val dynamoDBAsyncClient: AmazonDynamoDBAsync,
    override var logger: Logger? = null
) : DBFile {

    private var lastSetKeys: List<RecordField> = emptyList()
    private var includeFirst: Boolean = true
    private var lastSetOperation: Boolean = false
    private var eof: Boolean = false
    private var lastNativeMethod: NativeMethod? = null
    private val table: Table = DynamoDB(dynamoDBAsyncClient).getTable(name)
    private var primaryKey: String? = null

    private var lastEvaluatedKey: Map<String, Any>? = null
    private var currentItems: Iterator<Item>? = null


    private fun logEvent(loggingKey: LoggingKey, message: String, elapsedTime: Long? = null) =
        logger?.logEvent(loggingKey, message, elapsedTime, lastNativeMethod, fileMetadata.name)

    override fun eof(): Boolean {
        return eof
    }

    override fun equal(): Boolean {
        return if (!lastSetOperation) {
            false
        } else {
            val result = fetchNextItem()
            result != null && matchKeys(result, lastSetKeys)
        }
    }

    override fun setll(key: String): Boolean {
        return setll(listOf(key))
    }

    override fun setll(keys: List<String>): Boolean {
        lastNativeMethod = NativeMethod.setll
        logEvent(LoggingKey.native_access_method, "Executing setll on keys $keys")
        measureTimeMillis {
            eof = false
            lastEvaluatedKey = null
            val keyAsRecordField = mapKeysToRecordFields(keys)
            if (!fileMetadata.matchFileKeys(keyAsRecordField)) {
                return false
            }
            lastSetKeys = keyAsRecordField
            includeFirst = true
            lastSetOperation = true
            currentItems = executeScan(buildScanSpec(keyAsRecordField, true)).iterator()
        }.apply {
            logEvent(LoggingKey.native_access_method, "setll executed", this)
        }
        return currentItems!!.hasNext()
    }

    override fun setgt(key: String): Boolean {
        return setgt(listOf(key))
    }

    override fun setgt(keys: List<String>): Boolean {
        lastNativeMethod = NativeMethod.setgt
        logEvent(LoggingKey.native_access_method, "Executing setgt on keys $keys")
        measureTimeMillis {
            eof = false
            lastEvaluatedKey = null
            val keyAsRecordField = mapKeysToRecordFields(keys)
            if (!fileMetadata.matchFileKeys(keyAsRecordField)) {
                return false
            }
            lastSetKeys = keyAsRecordField
            includeFirst = false
            lastSetOperation = true
            currentItems = executeScan(buildScanSpec(keyAsRecordField, false)).iterator()
        }.apply {
            logEvent(LoggingKey.native_access_method, "setgt executed", this)
        }
        return currentItems!!.hasNext()
    }

    override fun chain(key: String): Result {
        return chain(listOf(key))
    }

    override fun chain(keys: List<String>): Result {
        val telemetrySpan = TelemetrySpan("CHAIN Execution")
        lastNativeMethod = NativeMethod.chain
        logEvent(LoggingKey.native_access_method, "Executing chain on keys $keys")
        var item: Item? = null
        measureTimeMillis {
            eof = false
            lastEvaluatedKey = null
            val keyAsRecordField = mapKeysToRecordFields(keys)
            if (!fileMetadata.matchFileKeys(keyAsRecordField)) {
                return Result(record = Record(), indicatorLO = true)
            }
            lastSetKeys = keyAsRecordField
            currentItems = executeScan(buildScanSpec(keyAsRecordField, true)).iterator()
            item = currentItems?.next()
        }.apply {
            logEvent(LoggingKey.native_access_method, "chain executed, result: $item", this)
        }
        telemetrySpan.endSpan()
        return if (item == null) {
            eof = true
            Result(Record())
        } else {
            Result(documentToRecord(item!!))
        }
    }

    override fun read(): Result {
        val telemetrySpan = TelemetrySpan("READ Execution")
        lastNativeMethod = NativeMethod.read
        logEvent(LoggingKey.native_access_method, "Executing read")

        var resultItem: Item? = null
        measureTimeMillis {
            resultItem = fetchNextItem()
        }.apply {
            logEvent(LoggingKey.native_access_method, "read executed, result: $resultItem", this)
        }
        telemetrySpan.endSpan()
        return if (resultItem != null) {
            eof = false
            Result(record = documentToRecord(resultItem!!))
        } else {
            eof = true
            Result(indicatorLO = true, errorMsg = "READ called on EOF cursor")
        }.also {
            lastNativeMethod = null
        }
    }

    override fun readEqual(): Result {
        return readEqual(lastSetKeys.map { it.value })
    }

    override fun readEqual(key: String): Result {
        return readEqual(listOf(key))
    }

    override fun readEqual(keys: List<String>): Result {
        val telemetrySpan = TelemetrySpan("READE Execution")
        lastNativeMethod = NativeMethod.readEqual
        logEvent(LoggingKey.native_access_method, "Executing readEqual on keys $keys")

        var isMatch: Boolean
        var resultItem: Item? = null
        measureTimeMillis {
            val keyAsRecordField = mapKeysToRecordFields(keys)

            if (!fileMetadata.matchFileKeys(keyAsRecordField)) {
                return Result(indicatorLO = true, errorMsg = "READE keys not matching file primary keys")
            }

            resultItem = fetchNextItem()
            isMatch = resultItem != null && matchKeys(resultItem!!, keyAsRecordField)
        }.apply {
            logEvent(LoggingKey.native_access_method, "readEqual executed, result: $resultItem", this)
        }
        telemetrySpan.endSpan()
        return if (isMatch && resultItem != null) {
            eof = false
            Result(record = documentToRecord(resultItem!!))
        } else {
            eof = true
            Result(indicatorHI = true)
        }.also {
            lastNativeMethod = null
        }
    }

    override fun readPrevious(): Result {
        TODO("Not yet implemented")
    }

    override fun readPreviousEqual(): Result {
        TODO("Not yet implemented")
    }

    override fun readPreviousEqual(key: String): Result {
        TODO("Not yet implemented")
    }

    override fun readPreviousEqual(keys: List<String>): Result {
        TODO("Not yet implemented")
    }


    override fun write(record: Record): Result {
        lastNativeMethod = NativeMethod.write
        val telemetrySpan = TelemetrySpan("WRITE Execution")
        logEvent(LoggingKey.native_access_method, "Executing write")

        return try {
            measureTimeMillis {
                val item = recordToItem(record)

                val expressionAttributeNames = mutableMapOf<String, String>()

                val conditionExpression = fileMetadata.fileKeys.mapIndexed { index, key ->
                    val placeholderName = "#key$index"  // Placeholder for the attribute name

                    // Map the placeholder to the actual attribute name
                    expressionAttributeNames[placeholderName] = key

                    "attribute_not_exists($placeholderName)"
                }.joinToString(" AND ")


                val putItemSpec = PutItemSpec()
                    .withItem(item)
                    .withConditionExpression(conditionExpression)
                    .withNameMap(expressionAttributeNames)

                table.putItem(putItemSpec)
            }.apply {
                logEvent(LoggingKey.native_access_method, "write executed", this)
            }

            telemetrySpan.endSpan()

            Result(record = record).also {
                lastNativeMethod = null
            }
        } catch (e: Exception) {
            val errorMsg = if (e is ConditionalCheckFailedException) {
                "Record already exists"
            } else {
                "Error writing record: ${e.message}"
            }

            Result(indicatorLO = true, errorMsg = errorMsg).also {
                logEvent(LoggingKey.native_access_method, "write failed", elapsedTime = null)
                lastNativeMethod = null
            }
        }
    }


    override fun update(record: Record): Result {
        lastNativeMethod = NativeMethod.update
        val telemetrySpan = TelemetrySpan("UPDATE Execution")
        logEvent(LoggingKey.native_access_method, "Executing update")

        return try {
            measureTimeMillis {            // Build the primary key
                val key = getPrimaryKey(tableName = name)
                val keyValue = record.entries.firstOrNull { it.key == key }?.value
                val primaryKey = PrimaryKey(key, keyValue)


                // Prepare the AttributeUpdates for each field in the record except primary keys
                val attributeUpdates = record.entries
                    .filterNot { (it.key) !== key } // Skip primary key field
                    .map { field ->
                        AttributeUpdate(field.key).put(field.value) // Create AttributeUpdate for each non-key field
                    }.toTypedArray()

                table.updateItem(primaryKey, *attributeUpdates)
            }.apply {
                logEvent(LoggingKey.native_access_method, "update executed", this)
            }
            telemetrySpan.endSpan()
            Result(record = record).also {
                lastNativeMethod = null
            }
        } catch (e: Exception) {
            // Handle errors and return a failure result
            Result(indicatorLO = true, errorMsg = "Error updating record: ${e.message}").also {
                logEvent(LoggingKey.native_access_method, "update failed", elapsedTime = null)
                lastNativeMethod = null
            }
        }
    }


    override fun delete(record: Record): Result {
        lastNativeMethod = NativeMethod.delete
        val telemetrySpan = TelemetrySpan("DELETE Execution")
        logEvent(LoggingKey.native_access_method, "Executing delete")

        return try {
            measureTimeMillis { // Build the primary key
                val key = getPrimaryKey(tableName = name)
                val keyValue = record.entries.firstOrNull { it.key == key }?.value
                val primaryKey = PrimaryKey(key, keyValue)

                // Perform the delete
                table.deleteItem(primaryKey)
            }.apply {
                logEvent(LoggingKey.native_access_method, "delete executed", this)
            }

            telemetrySpan.endSpan()

            Result(record = record).also {
                lastNativeMethod = null
            }
        } catch (e: Exception) {
            Result(indicatorLO = true, errorMsg = "Error deleting record: ${e.message}").also {
                logEvent(LoggingKey.native_access_method, "delete failed", elapsedTime = null)
                lastNativeMethod = null
            }
        }
    }

    fun getPrimaryKey(tableName: String): String? {

        if (primaryKey.isNullOrEmpty()) {
            val describeTableRequest = DescribeTableRequest().withTableName(tableName)

            logEvent(LoggingKey.search_data, "Retrieving $tableName Primary Key")

            val describeTableResult: DescribeTableResult = dynamoDBAsyncClient.describeTable(describeTableRequest)

            val keySchema = describeTableResult.table.keySchema

            primaryKey = keySchema.firstOrNull()?.attributeName
        }

        return primaryKey


    }


    private fun fetchNextItem(): Item? {
        if (currentItems == null || !currentItems!!.hasNext()) {
            if (lastEvaluatedKey != null) {
                // Continue the scan from the last evaluated key
                val scanSpec = buildScanSpec(lastSetKeys, true).withExclusiveStartKey(
                    PrimaryKey(
                        KeyAttribute(
                            lastEvaluatedKey!!.keys.firstOrNull(), lastEvaluatedKey!!.values.firstOrNull()
                        )
                    )
                )
                currentItems = executeScan(scanSpec).iterator()
            }
        }
        return if (currentItems != null && currentItems!!.hasNext()) {
            currentItems!!.next().also {
                lastEvaluatedKey = it.asMap() // Save the last evaluated key
            }
        } else {
            null
        }
    }

    private fun documentToRecord(item: Item): Record {
        val record = Record()
        item.asMap().forEach { (k, v) ->
            record.add(RecordField(k, v.toString()))
        }
        return record
    }

    private fun recordToItem(record: Record): Item {
        val item = Item()

        // TODO maybe handle different types
        record.entries.forEach { field ->
            item.withString(field.key, field.value)
        }

        return item
    }

    private fun matchKeys(item: Item, keys: List<RecordField>): Boolean {
        return keys.all { key ->
            item.get(key.name) == key.value
        }
    }

    private fun mapKeysToRecordFields(keys: List<String>): List<RecordField> {
        return keys.mapIndexed { index, key ->
            RecordField(fileMetadata.fileKeys[index], key)
        }
    }

    private fun buildScanSpec(keys: List<RecordField>, inclusive: Boolean): ScanSpec {
        val expressionAttributeNames = mutableMapOf<String, String>()
        val expressionAttributeValues = mutableMapOf<String, String>()

        val filterExpression = keys.mapIndexed { index, key ->
            val placeholderName = "#key$index"  // Placeholder for the attribute name
            val placeholderValue = ":value$index" // Placeholder for the attribute value

            // Map the placeholder to the actual attribute name
            expressionAttributeNames[placeholderName] = key.name

            // Map the placeholder to the actual value
            expressionAttributeValues[placeholderValue] = (key.value)

            "$placeholderName = $placeholderValue"
        }.joinToString(" AND ")

        val scanSpec = ScanSpec()
            .withFilterExpression(filterExpression)
            .withNameMap(expressionAttributeNames)
            .withValueMap(expressionAttributeValues as Map<String, Any>?)

        if (inclusive) {
            scanSpec.withConsistentRead(true)
        }

        return scanSpec
    }

    private fun executeScan(scanSpec: ScanSpec): Iterable<Item> {
        return try {
            val scanSpecDetails = buildString {
                append("Executing scan with details: [")
                append("FilterExpression: ${scanSpec.filterExpression}, ")
                append("NameMap: ${scanSpec.nameMap}, ")
                append("ValueMap: ${scanSpec.valueMap}, ")
                append("ExclusiveStartKey: ${scanSpec.exclusiveStartKey}, ")
                append("AttributesToGet: ${scanSpec.attributesToGet?.joinToString(", ")}, ")
                append("ConsistentRead: ${scanSpec.isConsistentRead}, ")
                append("ProjectionExpression: ${scanSpec.projectionExpression ?: "Not set"} ")
                append("]")
            }

            logEvent(LoggingKey.search_data, scanSpecDetails)

            return table.scan(scanSpec)
        } catch (e: Exception) {
            emptyList<Item>().asIterable() // Handle errors gracefully
        }
    }
}
