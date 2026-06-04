package com.smeup.dbnative

import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.model.FileMetadata
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

private const val TEST_APP = "test"
private const val OTHER_APP = "other"

private val TEST_CONFIG = ConnectionConfig(
    fileName = "*",
    url = "class:com.smeup.dbnative.mock.MockDBManager",
    user = "",
    password = ""
)

private val TEST_CONFIG_B = ConnectionConfig(
    fileName = "FILEB",
    url = "class:com.smeup.dbnative.mock.MockDBManager",
    user = "b",
    password = "b"
)

private val ACCESS_CONFIG = DBNativeAccessConfig(listOf(TEST_CONFIG_B, TEST_CONFIG))
private val CONFIG_MAP = mapOf(TEST_APP to ACCESS_CONFIG)

private fun spyFactory(configMap: Map<String, DBNativeAccessConfig>): Map<String, (ConnectionConfig) -> DBMManager> =
    configMap.mapValues { { connConfig: ConnectionConfig -> SpyDBMManager(connConfig) } }

/**
 * Unit tests for [ConnectionProvider] scoped lifecycle and lookup behavior.
 */
class ConnectionProviderTest {

    @Before
    fun setUp() {
        ConnectionProvider.configure(CONFIG_MAP, spyFactory(CONFIG_MAP))
    }

    @After
    fun tearDown() {
        // Reset state between tests by re-configuring; the real clean-up is in withScope's finally block.
    }

    @Test
    fun withScope_createsManagerOnDemand() {
        ConnectionProvider.withScope(TEST_APP) {
            val manager = ConnectionProvider.currentManager("FILEA")
            assertNotNull(manager)
        }
    }

    @Test
    fun withScope_sameManagerForSameFile() {
        ConnectionProvider.withScope(TEST_APP) {
            val m1 = ConnectionProvider.currentManager("FILEA")
            val m2 = ConnectionProvider.currentManager("FILEA")
            assertSame(m1, m2)
        }
    }

    @Test
    fun withScope_differentManagerForDifferentConfig() {
        ConnectionProvider.withScope(TEST_APP) {
            val mA = ConnectionProvider.currentManager("FILEA")
            val mB = ConnectionProvider.currentManager("FILEB")
            assertTrue(mA !== mB)
        }
    }

    @Test
    fun withScope_closesManagersOnExit() {
        val spies = mutableListOf<SpyDBMManager>()
        val collectingFactory = CONFIG_MAP.mapValues { { connConfig: ConnectionConfig ->
            SpyDBMManager(connConfig).also { spies.add(it) }
        } }
        ConnectionProvider.configure(CONFIG_MAP, collectingFactory)
        ConnectionProvider.withScope(TEST_APP) {
            ConnectionProvider.currentManager("FILEA")
            ConnectionProvider.currentManager("FILEB")
        }
        assertTrue(spies.isNotEmpty())
        assertTrue(spies.all { it.closed })
    }

    @Test
    fun currentManagerOrNull_returnsNullOutsideScope() {
        val result = ConnectionProvider.currentManagerOrNull("FILEA")
        assertNull(result)
    }

    @Test
    fun currentManagerOrNull_returnsNullForUnknownFile() {
        val isolatedConfig = DBNativeAccessConfig(listOf(TEST_CONFIG_B))
        val isolatedMap = mapOf(TEST_APP to isolatedConfig)
        ConnectionProvider.configure(isolatedMap, spyFactory(isolatedMap))
        ConnectionProvider.withScope(TEST_APP) {
            val result = ConnectionProvider.currentManagerOrNull("UNKNOWN_NO_WILDCARD")
            assertNull(result)
        }
    }

    @Test
    fun withScope_throwsWhenNotConfigured() {
        val emptyConfig = DBNativeAccessConfig(listOf())
        val emptyMap = mapOf(TEST_APP to emptyConfig)
        ConnectionProvider.configure(emptyMap, emptyMap.mapValues { { cfg: ConnectionConfig -> SpyDBMManager(cfg) } })

        assertFailsWith<IllegalArgumentException> {
            ConnectionProvider.withScope(TEST_APP) {
                ConnectionProvider.currentManager("ANYTHING")
            }
        }
    }

    @Test
    fun isScopeActive_falseOutsideScope() {
        assertFalse(ConnectionProvider.isScopeActive())
    }

    @Test
    fun isScopeActive_trueInsideScope() {
        ConnectionProvider.withScope(TEST_APP) {
            assertTrue(ConnectionProvider.isScopeActive())
        }
    }

    @Test
    fun withScope_scopeIsThreadLocal() {
        var managerSeenFromOtherThread: DBMManager? = null
        ConnectionProvider.withScope(TEST_APP) {
            val thread = Thread {
                managerSeenFromOtherThread = ConnectionProvider.currentManagerOrNull("FILEA")
            }
            thread.start()
            thread.join()
        }
        assertNull(managerSeenFromOtherThread)
    }

    @Test
    fun withScope_differentAppsUseIsolatedConfigs() {
        val otherConfig = DBNativeAccessConfig(listOf(
            ConnectionConfig(fileName = "*", url = "class:com.smeup.dbnative.mock.MockDBManager", user = "other", password = "")
        ))
        val multiAppMap = mapOf(TEST_APP to ACCESS_CONFIG, OTHER_APP to otherConfig)
        ConnectionProvider.configure(multiAppMap, spyFactory(multiAppMap))

        ConnectionProvider.withScope(TEST_APP) {
            val m = ConnectionProvider.currentManager("FILEA") as SpyDBMManager
            assertTrue(m.connectionConfig.user == "")
        }
        ConnectionProvider.withScope(OTHER_APP) {
            val m = ConnectionProvider.currentManager("FILEA") as SpyDBMManager
            assertTrue(m.connectionConfig.user == "other")
        }
    }

    @Test
    fun currentManagerOrNull_returnsNullForUnknownApp() {
        ConnectionProvider.withScope("no-such-app") {
            val result = ConnectionProvider.currentManagerOrNull("FILEA")
            assertNull(result)
        }
    }
}

/**
 * Test double used to verify manager creation and closure semantics.
 */
class SpyDBMManager(override val connectionConfig: ConnectionConfig) : DBMManager {
    var closed = false

    override fun existFile(name: String) = false
    override fun registerMetadata(metadata: FileMetadata, overwrite: Boolean) {}
    override fun metadataOf(name: String): FileMetadata = throw NotImplementedError()
    override fun openFile(name: String): DBFile = throw NotImplementedError()
    override fun closeFile(name: String) {}
    override fun unregisterMetadata(name: String) {}
    override fun validateConfig() {}
    override fun close() { closed = true }
}
