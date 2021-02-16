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

package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBMManager
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.FileMetadata

/**
 * Factory for DBFile.
 * @param config Configuration
 * @param fileNameNormalizer Normalizer for fileName. Default implementation convert / with .
 * Usage:
 * ```
 *    val config = DBNativeAccessConfig(...)
 *    val factory = DBFileFactory(config)
 *    val file = factory.open("TESTTABLE")
 *    file.read()
 * ```
 * @see open
 * */
class DBFileFactory(
    private val config: DBNativeAccessConfig,
    private val fileNameNormalizer: (String) -> String = {it}
) : AutoCloseable {

    private val managers = mutableMapOf<ConnectionConfig, DBMManager> ()

    /**
     * Open the file named fileName. A file can only be opened after registration of its metadata.
     * Registering a metadata can be done by passing the metadata into the open request or by calling the registerMetadata function
     * before opening invocation. The file metadata recording is persistent, so after a first recording you can call the opening of the file
     * without further registration.
     *
     * @param fileName file to open
     * @param fileMetadata metadata to register. If passed, file metadata is registered before file opening
     * */
    fun open(fileName: String, fileMetadata: FileMetadata?) : DBFile {
        val fileNameNormalized = fileNameNormalizer(fileName)
        val configMatch = findConnectionConfigFor(fileNameNormalized, config.connectionsConfig)
        val dbmManager = managers.getOrPut(configMatch) {createDBManager(configMatch, config.logger).apply { validateConfig() }}

        if (fileMetadata != null) {
            dbmManager.registerMetadata(fileMetadata, true)
        }
        val dbFile = dbmManager.openFile(fileNameNormalized)
        return DBFileWrapper(dbFile = dbFile, dbmManager = dbmManager)
    }

    companion object {
        /**
         * Register file metadata
         * @param fileMetadata metadata to register
         */
        fun registerMetadata(fileMetadata: FileMetadata) {
            DBManagerBaseImpl.staticRegisterMetadata(fileMetadata, true)
        }

        /**
         * Unregister file metadata
         * @param fileMetadata metadata to register
         */
        fun unregisterMetadata(fileMetadataName: String) {
            DBManagerBaseImpl.staticUnregisterMetadata(fileMetadataName)
        }

        val COMPARATOR = ConnectionConfigComparator()
    }

    override fun close() {
        managers.values.forEach {it.close()}
    }
}

/**
 * Find a ConnectionConfig for file
 * @param fileName file name
 * @param connectionsConfig ConnectionConfig entries
 * */
fun findConnectionConfigFor(fileName: String, connectionsConfig: List<ConnectionConfig>) : ConnectionConfig {
    val configList = connectionsConfig.filter {
        it.fileName.toUpperCase() == fileName.toUpperCase() || it.fileName == "*" ||
                fileName.toUpperCase().matches(Regex(it.fileName.toUpperCase().replace("*", ".*")))
    }
    require(configList.isNotEmpty()) {
        "Wrong configuration. Not found a ConnectionConfig entry matching name: $fileName"
    }
    //At the top of the list we have ConnectionConfig whose property file does not have wildcards
    return configList.sortedWith(DBFileFactory.COMPARATOR)[0]
}

private fun createDBManager(config: ConnectionConfig, logger: Logger? = null): DBMManager {
    val impl = getImplByUrl(config)

    val clazz :Class<DBMManager>? = Class.forName(impl) as Class<DBMManager>?

    return clazz?.let {
        val constructor = it.getConstructor(ConnectionConfig::class.java)
        val dbmManager = constructor.newInstance(config)
        if(dbmManager is DBManagerBaseImpl){
            dbmManager.logger = logger
        }
        return dbmManager
    }!!
}

private fun getImplByUrl(config: ConnectionConfig) : String {
    return when {
        config.impl != null && config.impl!!.trim().isEmpty() -> config.impl!!
        config.url.startsWith("jdbc:") -> "com.smeup.dbnative.sql.SQLDBMManager"
        config.url.startsWith("mongodb:") -> "com.smeup.dbnative.nosql.NoSQLDBMManager"
        config.url.startsWith("as400:") -> "com.smeup.dbnative.jt400.JT400DBMMAnager"
        else -> throw IllegalArgumentException("${config.url} not handled")
    }
}

//ConnectionConfig.file with wildcards at the bottom
class ConnectionConfigComparator : Comparator<ConnectionConfig> {

    override fun compare(o1: ConnectionConfig?, o2: ConnectionConfig?): Int {
        require(o1 != null)
        require(o2 != null)
        return when {
            o1.fileName == "*" && o2.fileName != "*" -> 1
            o1.fileName != "*" && o2.fileName == "*" -> -1
            o1.fileName.contains("*") && o2.fileName.contains("*") -> o1.fileName.compareTo(o2.fileName)
            o1.fileName.contains("*") && !o2.fileName.contains("*") -> 1
            !o1.fileName.contains("*") && o2.fileName.contains("*") -> -1
            else -> o1.fileName.compareTo(o2.fileName)
        }
    }
}