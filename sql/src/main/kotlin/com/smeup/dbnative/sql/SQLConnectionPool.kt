package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection

/**
 * Hikari-backed pool for SQL connections built from a [ConnectionConfig].
 */
open class SQLConnectionPool(private val connectionConfig: ConnectionConfig) : AutoCloseable {

    private val hikariDataSource: HikariDataSource = run {
        val pool = connectionConfig.poolConfig
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
            initializationFailTimeout = -1  // don't validate connection eagerly at pool creation
            connectionConfig.properties.forEach { (k, v) -> addDataSourceProperty(k, v) }
        }
        HikariDataSource(hikariConfig)
    }

    /**
     * Borrows a connection from the pool.
     */
    open fun getConnection(): Connection = hikariDataSource.connection

    override fun close() = hikariDataSource.close()
}
