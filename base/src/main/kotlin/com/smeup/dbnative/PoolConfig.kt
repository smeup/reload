package com.smeup.dbnative

/**
 * Connection pool tuning values used by SQL-backed managers.
 *
 * @param maximumPoolSize Maximum number of active connections.
 * @param minimumIdle Minimum number of idle connections kept in the pool.
 * @param connectionTimeoutMs Maximum wait time in milliseconds to borrow a connection.
 * @param idleTimeoutMs Maximum idle time in milliseconds before a connection can be evicted.
 * @param maxLifetimeMs Maximum lifetime in milliseconds for a pooled connection.
 */
data class PoolConfig @JvmOverloads constructor(
    val maximumPoolSize: Int = 10,
    val minimumIdle: Int = 2,
    val connectionTimeoutMs: Long = 30_000,
    val idleTimeoutMs: Long = 600_000,
    val maxLifetimeMs: Long = 1_800_000
)
