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

import com.mongodb.MongoClient
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

    // Best regex "mongodb://(?:([a-zA-Z0-9._%+-]+):([a-zA-Z0-9._%+-]+)@)?([a-zA-Z0-9.-]+):(\\d+)/([a-zA-Z0-9._-]+)$"

    //private val match = Regex("mongodb://((?:\\w|\\.)+):(\\d+)/(\\w+)").find(connectionConfig.url)
    private var host : String = ""
    private var port : Int = 0
    private var dataBase : String = ""
    private var username : String? = ""
    private var password : String? = ""

    private val match = parseConnectionString(connectionConfig.url)

    private fun parseConnectionString(connectionUrl: String): Boolean  {

        // Define each part of the regex as separate variables
        val schemePart = "^mongodb:\\/\\/"
        val userInfoPart = "(?:([a-zA-Z0-9._%+-]+):([a-zA-Z0-9._%+-]+)@)?"
        val hostPart = "([a-zA-Z0-9.-]+)"
        val portPart = ":(\\d+)"
        val databasePart = "\\/([a-zA-Z0-9._-]+)"

        // Combine the elements to form the complete regex pattern
        val fullRegexPattern = "$schemePart$userInfoPart$hostPart$portPart$databasePart$"

        // Create a Regex object using the combined pattern
        val tmpRegex = Regex(fullRegexPattern)

        // Match the connection string against the regex
        val matchResult = tmpRegex.matchEntire(connectionUrl)

        // If there's a match, extract the values and return them as a data class
        if (matchResult != null) {
            username = matchResult.groups[1]?.value
            password = matchResult.groups[2]?.value
            host = matchResult.groups[3]?.value!!
            port = matchResult.groups[4]?.value!!.toInt()
            dataBase = matchResult.groups[5]?.value!!
            return true
        } else return false
    }

    private val mongoClient : MongoClient by lazy {
        MongoClient(connectionConfig.url)
    }

    val mongoDatabase : MongoDatabase by lazy {
        mongoClient.getDatabase(dataBase)
    }

    private var openedFile = mutableMapOf<String, NoSQLDBFile>()

    override fun validateConfig() {
        if (!match) {
            throw  RuntimeException("Url syntax is not valid, correct format is: mongodb:user:password@//host:port/database")
        }
    }

    override fun close() {
        openedFile.values.forEach { it.close()}
        openedFile.clear()
        mongoClient.close()
    }

    override fun openFile(name: String): DBFile {

        require(existFile(name)) {
            "Cannot open unregistered file $name"
        }

        val sqldbFile: NoSQLDBFile
        val key = name

        if (openedFile.containsKey(key)) {
            sqldbFile = openedFile.getValue(key)
        } else {
            require(existFile(name)) {
                "File $name do not exist"
            }
            sqldbFile = NoSQLDBFile(name, metadataOf(name), mongoDatabase, logger)
            openedFile.putIfAbsent(key, sqldbFile)
        }
        return sqldbFile
    }

    override fun closeFile(name: String) {
        openedFile.remove(name)!!.close()
    }
}