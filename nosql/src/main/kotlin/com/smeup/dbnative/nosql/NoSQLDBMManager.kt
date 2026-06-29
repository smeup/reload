/*
 * Copyright 2020 The Reload project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.smeup.dbnative.nosql

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.file.DBFile

/**
 * Assign table:
 *
 * Library (i.e W_TEST) --> Database
 * File (i.e. BRARTIOF) --> Collection
 * Record --> Object in collection
 */

class NoSQLDBMManager (override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl<Nothing, Nothing>() {

    // MongoDB-related fields
    private var host: String = ""
    private var port: Int = 0
    private var dataBase: String = ""
    private var username: String? = ""
    private var password: String? = ""
    private lateinit var mongoClient: MongoClient
    lateinit var mongoDatabase: MongoDatabase

    private val openedFiles = mutableMapOf<String, DBFile>()

    init {
        validateConfig()
        setupMongoClient()
    }

    private fun setupMongoClient() {
        // The check is already in validateConfig, here we just connect
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

    override fun validateConfig() {
        if (connectionConfig.url.isBlank()) {
            throw RuntimeException("Database endpoint URL is required")
        }
        if (!connectionConfig.url.startsWith("mongodb", ignoreCase = true) || !parseMongoConnectionString(connectionConfig.url)) {
            throw RuntimeException("Invalid MongoDB URL format. Expected: mongodb://[username:password@]host:port/database")
        }
    }

    override fun close() {
        openedFiles.values.forEach { it.close() }
        openedFiles.clear()
        mongoClient.close()
    }

    override fun openFile(name: String): DBFile {
        require(existFile(name)) {
            "Cannot open unregistered file $name"
        }

        return openedFiles.getOrPut(name) {
            NoSQLDBFile(name, metadataOf(name), mongoDatabase, logger)
        }
    }

    override fun closeFile(name: String) {
        openedFiles.remove(name)?.close()
    }
}