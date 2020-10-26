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

import com.smeup.reload.ConnectionConfig
import com.smeup.reload.DBManagerBaseImpl
import com.smeup.reload.model.FileMetadata
import java.sql.Connection
import java.sql.DriverManager

open class SQLDBMManager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl()  {

    private var sqlLog: Boolean = false

    private var openedFile = mutableMapOf<String, SQLDBFile>()

    val connection : Connection by lazy {
        connectionConfig.driver?.let {
            Class.forName(connectionConfig.driver)
        }
        //todo handle connection pool
        DriverManager.getConnection(connectionConfig.url, connectionConfig.user, connectionConfig.password)
    }

    override fun validateConfig() {
    }

    override fun close() {
        connection.close()
    }

    /*
    override fun metadataOf(name: String): FileMetadata {
        require(openedFile.containsKey(name)) {
            "File: $name doesn't exist."
        }
        return openedFile.get(name)!!.fileMetadata
    }

    override fun existFile(name: String): Boolean {
        val dbm: DatabaseMetaData = connection.metaData
        dbm.getTables(null, null, name, arrayOf("TABLE", "VIEW")).use {
            return it.next()
        }
    }
     */

    override fun createFile(metadata: FileMetadata) {
        connection.createStatement().use {
            it.execute(metadata.toSQL())
            var dbFile = SQLDBFile(name = metadata.tableName, fileMetadata = metadata, connection =  connection)
            openedFile.putIfAbsent(metadata.tableName, dbFile)
        }
        super.createFile(metadata)
    }

    override fun openFile(name: String) = openedFile.getOrPut(name) {
        require(existFile(name)) {
            "Cannot open a unregistered file $name"
        }
        SQLDBFile(name = name, fileMetadata = metadataOf(name), connection =  connection)
    }


    override fun closeFile(name: String) {
        openedFile.remove(name)
    }

    fun execute(sqlStatements: List<String>) {
        connection.createStatement().use { statement ->
            sqlStatements.forEach { sql -> statement.addBatch(sql) }
            statement.executeBatch()
        }
    }

    fun setSQLLog(on: Boolean) {
        sqlLog = on
    }

}