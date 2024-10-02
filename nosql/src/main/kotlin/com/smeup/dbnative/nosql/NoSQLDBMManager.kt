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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.file.DBFile

/**
 *  Assign table:
 *
 *  Library (i.e W_TEST) --> Database
 *  File (i.e. BRARTIOF) --> Collection
 *  Record --> Object in collection
 */

class NoSQLDBMManager (override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl() {

    private enum class DatabaseType {
        MONGO, DYNAMO
    }

    private val databaseType: DatabaseType = determineDatabaseType()

    // MongoDB-related fields
    private var host: String = ""
    private var port: Int = 0
    private var dataBase: String = ""
    private var username: String? = ""
    private var password: String? = ""
    private lateinit var mongoClient: MongoClient
    lateinit var mongoDatabase: MongoDatabase

    // DynamoDB-related fields
    lateinit var dynamoDBAsyncClient: AmazonDynamoDBAsync
    lateinit var dynamoDB: DynamoDB

    private val openedFiles = mutableMapOf<String, DBFile>()

    init {
        validateConfig()
        when (databaseType) {
            DatabaseType.MONGO -> setupMongoClient()
            DatabaseType.DYNAMO -> setupDynamoDBClient()
        }
    }

    private fun determineDatabaseType(): DatabaseType {
        return if (connectionConfig.url.startsWith("mongodb", ignoreCase = true)) {
            DatabaseType.MONGO
        } else {
            DatabaseType.DYNAMO
        }
    }

    private fun setupMongoClient() {
        val match = parseMongoConnectionString(connectionConfig.url)
        if (!match) {
            throw RuntimeException("Invalid MongoDB URL format")
        }

        mongoClient = MongoClients.create(connectionConfig.url)
        mongoDatabase = mongoClient.getDatabase(dataBase)
    }

    private fun parseMongoConnectionString(connectionUrl: String): Boolean {
        val schemePart = "^mongodb:\\/\\/"
        val userInfoPart = "(?:([a-zA-Z0-9._%+-]+):([a-zA-Z0-9._%+-]+)@)?"
        val hostPart = "([a-zA-Z0-9.-]+)"
        val portPart = ":(\\d+)"
        val databasePart = "\\/([a-zA-Z0-9._-]+)"
        val optionsPart = "(\\?.*)?"

        val fullRegexPattern = "$schemePart$userInfoPart$hostPart$portPart$databasePart$optionsPart$"
        val tmpRegex = Regex(fullRegexPattern)

        val matchResult = tmpRegex.matchEntire(connectionUrl)
        return matchResult?.let {
            username = it.groups[1]?.value
            password = it.groups[2]?.value
            host = it.groups[3]?.value!!
            port = it.groups[4]?.value!!.toInt()
            dataBase = it.groups[5]?.value!!
            true
        } ?: false
    }

    private fun setupDynamoDBClient() {
        val region = connectionConfig.properties["REGION"] ?: throw RuntimeException("REGION is required")
        val endpoint = connectionConfig.url

        val clientBuilder = AmazonDynamoDBAsyncClientBuilder.standard()
            .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(endpoint, region))


        val awsAccessKeyId = connectionConfig.properties["AWS_ACCESS_KEY_ID"]
            ?: throw RuntimeException("AWS_ACCESS_KEY_ID is required")
        val awsSecretAccessKey = connectionConfig.properties["AWS_SECRET_ACCESS_KEY"]
            ?: throw RuntimeException("AWS_SECRET_ACCESS_KEY is required")

        val credentials = BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)
        clientBuilder.withCredentials(AWSStaticCredentialsProvider(credentials))


        dynamoDBAsyncClient = clientBuilder.build()
        dynamoDB = DynamoDB(dynamoDBAsyncClient)
    }


    override fun validateConfig() {
        if (connectionConfig.url.isBlank()) {
            throw RuntimeException("Database endpoint URL is required")
        }
        when (databaseType) {
            DatabaseType.MONGO -> {
                if (!parseMongoConnectionString(connectionConfig.url)) {
                    throw RuntimeException("Invalid MongoDB URL format")
                }
            }

            DatabaseType.DYNAMO -> {
                if (connectionConfig.properties["AWS_ACCESS_KEY_ID"].isNullOrBlank()) {
                    throw RuntimeException("AWS_ACCESS_KEY_ID is required")
                }
                if (connectionConfig.properties["AWS_SECRET_ACCESS_KEY"].isNullOrBlank()) {
                    throw RuntimeException("AWS_SECRET_ACCESS_KEY is required")
                }

                if (connectionConfig.properties["REGION"].isNullOrBlank()) {
                    throw RuntimeException("REGION is required")
                }
            }
        }
    }

    override fun close() {
        openedFiles.values.forEach { it.close() }
        openedFiles.clear()

        when (databaseType) {
            DatabaseType.MONGO -> mongoClient.close()
            DatabaseType.DYNAMO -> dynamoDBAsyncClient.shutdown()
        }
    }

    override fun openFile(name: String): DBFile {
        require(existFile(name)) {
            "Cannot open unregistered file $name"
        }

        return openedFiles.getOrPut(name) {
            // Ensure metadata is not null
            val fileMetadata = metadataOf(name)
            require(fileMetadata != null) { "Metadata for file '$name' is null." }
            when (databaseType) {
                DatabaseType.MONGO -> NoSQLDBFile(name, fileMetadata, mongoDatabase, logger)
                DatabaseType.DYNAMO -> DynamoDBFile(name, fileMetadata, dynamoDBAsyncClient, logger)
            }
        }
    }

    override fun closeFile(name: String) {
        openedFiles.remove(name)?.close()
    }
}
