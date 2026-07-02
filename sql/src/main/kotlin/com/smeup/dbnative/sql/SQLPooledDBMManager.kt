package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.log.LoggingKey
import java.sql.Connection
import kotlin.system.measureTimeMillis

/**
 * [SQLDBMManager] that lazily borrows and releases its connection from [pool].
 */
class SQLPooledDBMManager(
    connectionConfig: ConnectionConfig,
    private val pool: SQLConnectionPool
) : SQLDBMManager(connectionConfig) {

    private val connectionLazy = lazy {
        val conn: Connection
        measureTimeMillis { conn = pool.getConnection() }.apply {
            logger?.logEvent(LoggingKey.connection, "Borrowed pooled connection from url ${connectionConfig.url}", this)
        }
        dialect.onConnectionOpened(conn)
        conn
    }
    override val connection: Connection by connectionLazy

    override fun close() = finishConnection(commit = true)

    override fun abort() = finishConnection(commit = false)

    private fun finishConnection(commit: Boolean) {
        if (connectionLazy.isInitialized()) {
            openedFiles.forEach { it.close() }
            openedFiles.clear()
            dialect.onConnectionClosing(connection, commit)
            logger?.logEvent(LoggingKey.connection, "Returning pooled connection to url ${connectionConfig.url}")
            connection.close()
        }
    }
}
