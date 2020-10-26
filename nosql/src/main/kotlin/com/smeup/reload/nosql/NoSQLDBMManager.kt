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

package com.smeup.reload.nosql

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.smeup.reload.ConnectionConfig
import com.smeup.reload.DBManagerBaseImpl
import com.smeup.reload.file.DBFile
import com.smeup.reload.model.FileMetadata
import com.smeup.reload.nosql.utils.buildIndexCommand
import com.smeup.reload.nosql.utils.toMongoDocument
import org.bson.Document

/**
 *  Assign table:
 *
 *  Library (i.e W_TEST) --> Database
 *  File (i.e. BRARTIOF) --> Collection
 *  Record --> Object in collection
 */

class NoSQLDBMManager (override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl() {

    private val match = Regex("mongodb://((?:\\w|\\.)+):(\\d+)/(\\w+)").find(connectionConfig.url)
    private val host : String by lazy {
        match!!.destructured.component1()
    }
    private val port : Int by lazy {
        match!!.destructured.component2().toInt()
    }
    private val dataBase : String by lazy {
        match!!.destructured.component3()
    }

    private val mongoClient : MongoClient by lazy {
        MongoClient(host, port)
    }

    private val mongoDatabase : MongoDatabase by lazy {
        mongoClient.getDatabase(dataBase)
    }

    private val metadataFile : MongoCollection<Document> by lazy{
        mongoDatabase.getCollection(METADATA_COLLECTION)
    }

    private var openedFile = mutableMapOf<String, NoSQLDBFile>()

    companion object {
        const val METADATA_COLLECTION = "METADATA"
    }

    override fun validateConfig() {
        require(match != null) {
            "Url syntax is not valid, correct format: mongodb://host:port/database"
        }
    }

    override fun close() {
        mongoClient.close()
    }

    /*
    override fun existFile(name: String): Boolean {
        return metadataFile.find(eq("name", name)).count() != 0
    }



    override fun metadataOf(name: String): FileMetadata {
        metadataFile.run {
            val iterableResult = find(eq("name", name))
            return iterableResult.first()!!.toMetadata()
        }
    }
    */


    override fun createFile(metadata: FileMetadata) {

        // Find table registration in library metadata file
        val whereQuery = BasicDBObject()
        whereQuery.put("name", metadata.tableName.toUpperCase())

        val cursor = metadataFile.find(whereQuery)

        if (cursor.count() == 0) {
            // Register file metadata
            metadataFile.insertOne(metadata.toMongoDocument())
            // Create file index
            mongoDatabase.runCommand(Document.parse(metadata.buildIndexCommand()))
        }

        super.createFile(metadata)
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
            sqldbFile = NoSQLDBFile(name, metadataOf(name), mongoDatabase)
            openedFile.putIfAbsent(key, sqldbFile)
        }
        return sqldbFile
    }

    override fun closeFile(name: String) {
        openedFile.remove(name)
    }

    fun existTableInMongoDB(name:String):Boolean {
        // Find table registration in library metadata file
        val whereQuery = BasicDBObject()
        whereQuery.put("name", name.toUpperCase())

        val cursor = metadataFile.find(whereQuery)

        return cursor.count() != 0
    }
}