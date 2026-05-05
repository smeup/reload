package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.PoolConfig
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

private val POOLED_CONNECTION_CONFIG = ConnectionConfig(
    fileName = "*",
    url = "jdbc:hsqldb:mem:POOLED_MGR_TEST",
    user = "sa",
    password = "root",
    poolConfig = PoolConfig(maximumPoolSize = 2, minimumIdle = 1)
)

/**
 * Tests pooled connection borrowing and release behavior of [SQLPooledDBMManager].
 */
class SQLPooledDBMManagerTest {

    private lateinit var pool: SQLConnectionPool

    @Before
    fun setUp() {
        pool = SQLConnectionPool(POOLED_CONNECTION_CONFIG)
    }

    @After
    fun tearDown() {
        pool.runCatching { close() }
    }

    @Test
    fun connection_borrowedFromPool() {
        val manager = SQLPooledDBMManager(POOLED_CONNECTION_CONFIG, pool)
        val conn = manager.connection
        assertNotNull(conn)
        assertFalse(conn.isClosed)
        manager.close()
    }

    @Test
    fun connection_lazyInitialization() {
        var callCount = 0
        val trackingPool = object : SQLConnectionPool(POOLED_CONNECTION_CONFIG) {
            override fun getConnection(): Connection {
                callCount++
                return super.getConnection()
            }
        }
        val manager = SQLPooledDBMManager(POOLED_CONNECTION_CONFIG, trackingPool)
        assertEquals(0, callCount, "Pool must not be called before connection is first accessed")
        manager.connection
        assertEquals(1, callCount)
        manager.connection
        assertEquals(1, callCount, "Lazy must reuse connection, not call pool again")
        manager.close()
    }

    @Test
    fun close_returnsConnectionToPool() {
        var closeCalled = false
        val trackingPool = object : SQLConnectionPool(POOLED_CONNECTION_CONFIG) {
            override fun getConnection(): Connection = object : Connection by super.getConnection() {
                override fun close() { closeCalled = true }
            }
        }
        val manager = SQLPooledDBMManager(POOLED_CONNECTION_CONFIG, trackingPool)
        manager.connection
        manager.close()
        assertFalse(!closeCalled, "close() must be called on the connection to return it to pool")
    }

    @Test
    fun close_doesNotCallPoolWhenConnectionNeverAccessed() {
        var callCount = 0
        val trackingPool = object : SQLConnectionPool(POOLED_CONNECTION_CONFIG) {
            override fun getConnection(): Connection {
                callCount++
                return super.getConnection()
            }
        }
        val manager = SQLPooledDBMManager(POOLED_CONNECTION_CONFIG, trackingPool)
        manager.close()
        assertEquals(0, callCount, "Pool must not be called when connection was never accessed")
    }
}
