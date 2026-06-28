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
import com.smeup.dbnative.log.TelemetrySpan
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import kotlin.system.measureTimeMillis

open class SQLDBMManager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl<SQLQuery, ResultSet>() {

    private var sqlLog: Boolean = false
    private var connectionOpenedAt: Long = 0L

    //private var openedFile = mutableMapOf<String, SQLDBFile>()

    open val connection: Connection by lazy {
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
            connectionOpenedAt = System.currentTimeMillis()
            logger?.logEvent(LoggingKey.connection, "SQL connection successfully opened", this)
        }
        conn
    }

    override fun validateConfig() {
    }

    override fun close() {
        //openedFile.values.forEach { it.close()}
        //openedFile.clear()
        val lifetime = if (connectionOpenedAt > 0L) System.currentTimeMillis() - connectionOpenedAt else null
        logger?.logEvent(LoggingKey.connection, "Closing SQL connection on url ${connectionConfig.url}", lifetime)
        connection.close()
    }

    override fun openFile(name: String): SQLDBFile {
        require(this.existFile(name))
        val dialect = SqlDialect.fromUrl(connectionConfig.url, connectionConfig.properties)
        return SQLDBFile(name = name, fileMetadata = metadataOf(name), connection = connection, logger = logger, dialect = dialect)
    }

    override fun closeFile(name: String) {
        //openedFile.remove(name)?.close()
    }

    fun execute(sqlStatements: List<String>) {
        connection.createStatement().use { statement ->
            sqlStatements.forEach { sql ->
                println(sql)
                statement.addBatch(sql)
            }
            statement.executeBatch()
        }
    }

    override fun <T> executeQuery(query: SQLQuery, block: (ResultSet) -> T): T {
        val telemetrySpan = TelemetrySpan("EXECUTE QUERY Execution")
        logger?.logEvent(LoggingKey.execute_inquiry, "Preparing statement for query: ${query.query} with bindings: ${query.parameters}")

        val stmt: PreparedStatement
        measureTimeMillis {
            stmt = connection.prepareStatement(query.query)
            stmt.bind(query.parameters.map { it ?: "" })
        }.apply {
            logger?.logEvent(LoggingKey.execute_inquiry, "Statement prepared, executing query for statement", this)
        }

        return stmt.use {
            val rs: ResultSet
            measureTimeMillis {
                rs = stmt.executeQuery()
            }.apply {
                logger?.logEvent(LoggingKey.execute_inquiry, "Query successfully executed", this)
            }

            val result: T
            measureTimeMillis {
                result = rs.use { block(it) }
            }.apply {
                logger?.logEvent(LoggingKey.execute_inquiry, "Consumer completed", this)
            }

            telemetrySpan.endSpan()
            result
        }
    }

    /**
     * WARNING: The caller is responsible for closing the returned [ResultSet] and the underlying
     * [PreparedStatement]. Failing to do so will cause resource leaks.
     * Prefer [executeQuery] with a lambda block instead — it handles cleanup automatically:
     *   executeQuery(query) { rs -> ... }
     */
    fun executeQuery(query: SQLQuery, resultSetType: Int, concurrency: Int): ResultSet {
        val telemetrySpan = TelemetrySpan("EXECUTE CURSOR QUERY Execution")
        logger?.logEvent(LoggingKey.execute_inquiry, "Preparing cursor query: ${query.query} with bindings: ${query.parameters}")
        val stmt: PreparedStatement
        measureTimeMillis {
            stmt = connection.prepareStatement(query.query, resultSetType, concurrency)
            stmt.bind(query.parameters.map { it ?: "" })
        }.apply {
            logger?.logEvent(LoggingKey.execute_inquiry, "Statement prepared", this)
        }
        val rs: ResultSet
        measureTimeMillis {
            rs = stmt.executeQuery()
        }.apply {
            logger?.logEvent(LoggingKey.execute_inquiry, "Cursor query executed", this)
        }
        telemetrySpan.endSpan()
        return rs
    }

    fun setSQLLog(on: Boolean) {
        sqlLog = on
    }

}