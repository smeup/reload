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
    // One pool per unique (url, user) — many tables share the same database
    val pools = config.connectionsConfig
        .distinctBy { it.url to it.user }
        .associateBy({ it.url to it.user }) { SQLConnectionPool(it) }
    configure(config) { connConfig ->
        SQLPooledDBMManager(connConfig, requireNotNull(pools[connConfig.url to connConfig.user]) { "No pool for ${connConfig.url}" })
    }
    return AutoCloseable { pools.values.forEach { it.close() } }
}
