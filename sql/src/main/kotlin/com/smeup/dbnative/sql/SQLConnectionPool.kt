package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingKey
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import kotlin.system.measureTimeMillis

/**
 * Hikari-backed pool for SQL connections built from a [ConnectionConfig].
 */
open class SQLConnectionPool(
    private val connectionConfig: ConnectionConfig,
    private val logger: Logger? = null
) : AutoCloseable {

    private val hikariDataSource: HikariDataSource = run {
        val pool = requireNotNull(connectionConfig.poolConfig) {
            "poolConfig must not be null to create SQLConnectionPool"
        }
        val hikariConfig = HikariConfig().apply {
            jdbcUrl = connectionConfig.url
            username = connectionConfig.user
            password = connectionConfig.password
            connectionConfig.driver?.let { driverClassName = it }
            maximumPoolSize = pool.maximumPoolSize
            minimumIdle = pool.minimumIdle
            connectionTimeout = pool.connectionTimeoutMs
            idleTimeout = pool.idleTimeoutMs
            maxLifetime = pool.maxLifetimeMs
            pool.connectionTestQuery?.let { setConnectionTestQuery(it) }
            initializationFailTimeout = -1  // don't validate connection eagerly at pool creation
            connectionConfig.properties.forEach { (k, v) -> addDataSourceProperty(k, v) }
        }
        HikariDataSource(hikariConfig)
    }

    /**
     * Borrows a connection from the pool.
     */
    open fun getConnection(): Connection {
        val conn: Connection
        measureTimeMillis { conn = hikariDataSource.connection }.apply {
            val mxBean = hikariDataSource.hikariPoolMXBean
            val stats = if (mxBean != null)
                " [pool active=${mxBean.activeConnections} idle=${mxBean.idleConnections} total=${mxBean.totalConnections} waiting=${mxBean.threadsAwaitingConnection}]"
            else ""
            logger?.logEvent(LoggingKey.connection, "Pool acquired connection from url=${connectionConfig.url}$stats", this)
        }
        return conn
    }

    override fun close() = hikariDataSource.close()
}
