package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.log.LoggingKey
import javax.sql.DataSource

/**
 * Returns a thread-scoped [DataSource] for [fileName], or `null` for non-SQL managers.
 */
fun ConnectionProvider.currentDataSource(fileName: String): DataSource? =
    currentManager(fileName).toDataSource()

/**
 * Returns a thread-scoped SQL [DataSource] for [fileName].
 *
 * @throws IllegalArgumentException if the resolved manager is not SQL-based.
 */
fun ConnectionProvider.requireDataSource(fileName: String): DataSource =
    requireNotNull(currentManager(fileName).toDataSource()) {
        "Manager for '$fileName' is not SQL-based"
    }

/**
 * Configures [ConnectionProvider] with pooled SQL managers.
 *
 * One pool is created per unique (`url`, `user`) pair.
 *
 * @return A handle that closes all created pools.
 */
fun ConnectionProvider.configureWithPool(config: DBNativeAccessConfig): AutoCloseable {
    // One pool per unique (url, user) — only for configs that have an explicit poolConfig
    val pools = config.connectionsConfig
        .filter { it.poolConfig != null }
        .distinctBy { it.url to it.user }
        .associateBy({ it.url to it.user }) { SQLConnectionPool(it, config.logger) }
    pools.forEach { (key, _) ->
        config.logger?.logEvent(LoggingKey.connection, "Created SQL connection pool for url=${key.first} user=${key.second}")
    }
    configure(config) { connConfig ->
        val pool = pools[connConfig.url to connConfig.user]
        val manager = if (pool != null) SQLPooledDBMManager(connConfig, pool)
                      else SQLDBMManager(connConfig)
        manager.logger = config.logger
        manager
    }
    return AutoCloseable {
        pools.entries.forEach { (key, pool) ->
            config.logger?.logEvent(LoggingKey.connection, "Shutting down SQL connection pool for url=${key.first} user=${key.second}")
            pool.close()
        }
    }
}
