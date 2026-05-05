package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.PoolConfig
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

private val POOL_CONNECTION_CONFIG = ConnectionConfig(
    fileName = "*",
    url = "jdbc:hsqldb:mem:POOL_TEST",
    user = "sa",
    password = "root",
    poolConfig = PoolConfig(maximumPoolSize = 2, minimumIdle = 1)
)

/**
 * Tests [SQLConnectionPool] basic connection lifecycle behavior.
 */
class SQLConnectionPoolTest {

    private lateinit var pool: SQLConnectionPool

    @Before
    fun setUp() {
        pool = SQLConnectionPool(POOL_CONNECTION_CONFIG)
    }

    @After
    fun tearDown() {
        pool.runCatching { close() }
    }

    @Test
    fun getConnection_returnsOpenConnection() {
        val conn = pool.getConnection()
        assertNotNull(conn)
        assertFalse(conn.isClosed)
        conn.close()
    }

    @Test
    fun getConnection_returnsConnectionFromPool() {
        val c1 = pool.getConnection()
        c1.close()
        val c2 = pool.getConnection()
        assertNotNull(c2)
        assertFalse(c2.isClosed)
        c2.close()
    }

    @Test
    fun close_shutsDownPool() {
        pool.close()
        assertFailsWith<Exception> {
            pool.getConnection()
        }
    }
}
