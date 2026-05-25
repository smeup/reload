package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.log.LoggingKey
import javax.sql.DataSource

/**
 * Returns a thread-scoped [DataSource] for [fileName], or `null` if no scope is active,
 * the provider is not configured, or the manager is not SQL-based.
 */
fun ConnectionProvider.currentDataSource(fileName: String): DataSource? =
    currentManagerOrNull(fileName)?.toDataSource()

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
 * Configures [ConnectionProvider] with pooled SQL managers for the `"default"` app key.
 *
 * @deprecated Use [configureWithPool] with an explicit app-keyed map instead.
 */
@Deprecated(
    message = "Use configureWithPool(configMap) with an explicit app-keyed map instead.",
    replaceWith = ReplaceWith("configureWithPool(mapOf(\"default\" to config))")
)
fun ConnectionProvider.configureWithPool(config: DBNativeAccessConfig): AutoCloseable =
    configureWithPool(mapOf("default" to config))

/**
 * Configures [ConnectionProvider] with pooled SQL managers.
 *
 * One HikariCP pool is created per unique (`url`, `user`) pair across all apps —
 * apps that share the same database share a single pool.
 *
 * @param configMap One [DBNativeAccessConfig] per application key.
 * @return A handle that closes all created pools on [AutoCloseable.close].
 */
fun ConnectionProvider.configureWithPool(configMap: Map<String, DBNativeAccessConfig>): AutoCloseable {
    val logger = configMap.values.firstNotNullOfOrNull { it.logger }
    val allConfigs = configMap.values.flatMap { it.connectionsConfig }

    val pools = allConfigs
        .filter { it.poolConfig != null }
        .distinctBy { it.url to it.user }
        .associateBy({ it.url to it.user }) { SQLConnectionPool(it, logger) }

    pools.forEach { (key, _) ->
        logger?.logEvent(LoggingKey.connection, "Created SQL connection pool for url=${key.first} user=${key.second}")
    }

    val factoryMap = configMap.mapValues { (_, cfg) ->
        { connConfig: ConnectionConfig ->
            val pool = pools[connConfig.url to connConfig.user]
            val manager = if (pool != null) SQLPooledDBMManager(connConfig, pool)
                          else SQLDBMManager(connConfig)
            manager.logger = cfg.logger
            manager
        }
    }
    configure(configMap, factoryMap)

    return AutoCloseable {
        pools.forEach { (key, pool) ->
            logger?.logEvent(LoggingKey.connection, "Shutting down SQL connection pool for url=${key.first} user=${key.second}")
            pool.close()
        }
    }
}
