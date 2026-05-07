package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
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
        .associateBy({ it.url to it.user }) { SQLConnectionPool(it) }
    configure(config) { connConfig ->
        val pool = pools[connConfig.url to connConfig.user]
        if (pool != null) SQLPooledDBMManager(connConfig, pool)
        else SQLDBMManager(connConfig)
    }
    return AutoCloseable { pools.values.forEach { it.close() } }
}
