package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.PoolConfig
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

private val SQL_CONFIG = ConnectionConfig(
    fileName = "*",
    url = "jdbc:hsqldb:mem:CP_SQL_TEST",
    user = "sa",
    password = "root"
)

private val SQL_CONFIG_B = ConnectionConfig(
    fileName = "FILEB",
    url = "jdbc:hsqldb:mem:CP_SQL_TEST_B",
    user = "sa",
    password = "root"
)

private val POOLED_CONFIG = ConnectionConfig(
    fileName = "POOLED",
    url = "jdbc:hsqldb:mem:CP_POOLED_TEST",
    user = "sa",
    password = "root",
    poolConfig = PoolConfig(maximumPoolSize = 2)
)

private fun sqlFactory(cfg: com.smeup.dbnative.ConnectionConfig): com.smeup.dbnative.DBMManager =
    SQLDBMManager(cfg)

/**
 * Tests SQL-specific [com.smeup.dbnative.ConnectionProvider] extensions.
 */
class ConnectionProviderSqlExtensionsTest {

    @Before
    fun setUp() {
        ConnectionProvider.configure(DBNativeAccessConfig(listOf(SQL_CONFIG)), ::sqlFactory)
    }

    @After
    fun tearDown() {
        ConnectionProvider.configure(DBNativeAccessConfig(listOf(SQL_CONFIG)), ::sqlFactory)
    }

    @Test
    fun requireDataSource_returnsScopedDataSource() {
        ConnectionProvider.withScope {
            val ds = ConnectionProvider.requireDataSource("FILEA")
            assertNotNull(ds)
        }
    }

    @Test
    fun requireDataSource_throwsForNonSqlManager() {
        val nonSqlConfig = ConnectionConfig(
            fileName = "NOSQL",
            url = "class:com.smeup.dbnative.mock.MockDBManager",
            user = "",
            password = ""
        )
        ConnectionProvider.configure(DBNativeAccessConfig(listOf(nonSqlConfig))) { cfg ->
            com.smeup.dbnative.mock.MockDBManager(cfg)
        }
        assertFailsWith<IllegalArgumentException> {
            ConnectionProvider.withScope {
                ConnectionProvider.requireDataSource("NOSQL")
            }
        }
    }

    @Test
    fun currentDataSource_returnsNullForNonSqlManager() {
        val nonSqlConfig = ConnectionConfig(
            fileName = "NOSQL2",
            url = "class:com.smeup.dbnative.mock.MockDBManager",
            user = "",
            password = ""
        )
        ConnectionProvider.configure(DBNativeAccessConfig(listOf(nonSqlConfig))) { cfg ->
            com.smeup.dbnative.mock.MockDBManager(cfg)
        }
        ConnectionProvider.withScope {
            val ds = ConnectionProvider.currentDataSource("NOSQL2")
            assertNull(ds)
        }
    }

    @Test
    fun getConnection_sameInstanceWithinScope() {
        ConnectionProvider.withScope {
            val ds = ConnectionProvider.requireDataSource("FILEA")
            val c1 = ds.getConnection()
            val c2 = ds.getConnection()
            assertSame(c1, c2)
        }
    }

    @Test
    fun configureWithPool_createsOnePoolPerConnectionConfig() {
        val config = DBNativeAccessConfig(listOf(SQL_CONFIG, SQL_CONFIG_B))
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                val dsA = ConnectionProvider.requireDataSource("FILEA")
                val dsB = ConnectionProvider.requireDataSource("FILEB")
                assertNotNull(dsA)
                assertNotNull(dsB)
            }
        } finally {
            handle.close()
        }
    }

    @Test
    fun configureWithPool_closeHandle_shutsDownAllPools() {
        val config = DBNativeAccessConfig(listOf(SQL_CONFIG, SQL_CONFIG_B))
        val handle = ConnectionProvider.configureWithPool(config)
        handle.close()
        // Re-configure with non-pooled to restore state for other tests
        ConnectionProvider.configure(DBNativeAccessConfig(listOf(SQL_CONFIG)), ::sqlFactory)
    }

    @Test
    fun configureWithPool_withExplicitPoolConfig_createsSQLPooledDBMManager() {
        val config = DBNativeAccessConfig(listOf(POOLED_CONFIG))
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                val manager = ConnectionProvider.currentManager("POOLED")
                assertTrue(manager is SQLPooledDBMManager)
            }
        } finally {
            handle.close()
        }
    }

    @Test
    fun configureWithPool_withoutPoolConfig_fallsBackToSQLDBMManager() {
        val config = DBNativeAccessConfig(listOf(SQL_CONFIG))
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                val manager = ConnectionProvider.currentManager("FILEA")
                assertTrue(manager is SQLDBMManager)
                assertFalse(manager is SQLPooledDBMManager)
            }
        } finally {
            handle.close()
        }
    }

    @Test
    fun configureWithPool_mixedConfigs_routesCorrectly() {
        val config = DBNativeAccessConfig(listOf(POOLED_CONFIG, SQL_CONFIG))
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                val pooledManager = ConnectionProvider.currentManager("POOLED")
                val nonPooledManager = ConnectionProvider.currentManager("FILEA")
                assertTrue(pooledManager is SQLPooledDBMManager)
                assertTrue(nonPooledManager is SQLDBMManager)
                assertFalse(nonPooledManager is SQLPooledDBMManager)
            }
        } finally {
            handle.close()
        }
    }
}
