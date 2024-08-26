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

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.log.LoggingKey
import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import kotlin.system.measureTimeMillis

open class SQLDBMManager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl() {

    private var sqlLog: Boolean = false

    //private var openedFile = mutableMapOf<String, SQLDBFile>()

    val connection: Connection by lazy {
        logger?.logEvent(LoggingKey.connection, "Opening SQL connection on url ${connectionConfig.url}")
        val conn: Connection
        measureTimeMillis {
            connectionConfig.driver?.let {
                Class.forName(connectionConfig.driver)
            }

            val connectionProps = Properties()
            connectionProps["user"] = connectionConfig.user
            connectionProps["password"] = connectionConfig.password

            connectionConfig.properties.forEach {
                if (it.key != "user" && it.key != "password") {
                    connectionProps[it.key] = it.value
                }
            }
            conn = DriverManager.getConnection(connectionConfig.url, connectionProps)
        }.apply {
            logger?.logEvent(LoggingKey.connection, "SQL connection successfully opened", this)
        }
        conn
    }

    override fun validateConfig() {
    }

    override fun close() {
        //openedFile.values.forEach { it.close()}
        //openedFile.clear()
        connection.close()
    }

    override fun openFile(name: String): SQLDBFile {
        require(this.existFile(name))
        return SQLDBFile(name = name, fileMetadata = metadataOf(name), connection = connection, logger)
    }
    fun openFileNoLog(name: String): SQLDBFileNoLog {
        require(this.existFile(name))
        return SQLDBFileNoLog(name = name, fileMetadata = metadataOf(name), connection = connection, null)
    }
    fun openFilePerf(name: String): SQLDBFilePerf {
        require(this.existFile(name))
        return SQLDBFilePerf(name = name, fileMetadata = metadataOf(name), connection = connection, null)
    }

    override fun closeFile(name: String) {
        //openedFile.remove(name)?.close()
    }

    fun execute(sqlStatements: List<String>) {
        connection.createStatement().use { statement ->
            sqlStatements.forEach {
                    sql ->
                println(sql)
                statement.addBatch(sql)
            }
            statement.executeBatch()
        }
    }

    fun setSQLLog(on: Boolean) {
        sqlLog = on
    }

}