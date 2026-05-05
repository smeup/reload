package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import java.sql.Connection

/**
 * [SQLDBMManager] that lazily borrows and releases its connection from [pool].
 */
class SQLPooledDBMManager(
    connectionConfig: ConnectionConfig,
    private val pool: SQLConnectionPool
) : SQLDBMManager(connectionConfig) {

    private val connectionLazy = lazy { pool.getConnection() }
    override val connection: Connection by connectionLazy

    override fun close() {
        if (connectionLazy.isInitialized()) {
            connection.close()
        }
    }
}
