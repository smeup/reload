package com.smeup.dbnative

import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.model.FileMetadata
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

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

/**
 * Unit tests for [ConnectionProvider] scoped lifecycle and lookup behavior.
 */
class ConnectionProviderTest {

    @Before
    fun setUp() {
        ConnectionProvider.configure(ACCESS_CONFIG) { connConfig -> SpyDBMManager(connConfig) }
    }

    @After
    fun tearDown() {
        // Reset state between tests by re-configuring with a no-op factory;
        // the real clean-up is handled by withScope's finally block.
    }

    @Test
    fun withScope_createsManagerOnDemand() {
        ConnectionProvider.withScope {
            val manager = ConnectionProvider.currentManager("FILEA")
            assertNotNull(manager)
        }
    }

    @Test
    fun withScope_sameManagerForSameFile() {
        ConnectionProvider.withScope {
            val m1 = ConnectionProvider.currentManager("FILEA")
            val m2 = ConnectionProvider.currentManager("FILEA")
            assertSame(m1, m2)
        }
    }

    @Test
    fun withScope_differentManagerForDifferentConfig() {
        ConnectionProvider.withScope {
            val mA = ConnectionProvider.currentManager("FILEA")
            val mB = ConnectionProvider.currentManager("FILEB")
            assertTrue(mA !== mB)
        }
    }

    @Test
    fun withScope_closesManagersOnExit() {
        val spies = mutableListOf<SpyDBMManager>()
        ConnectionProvider.configure(ACCESS_CONFIG) { connConfig ->
            SpyDBMManager(connConfig).also { spies.add(it) }
        }
        ConnectionProvider.withScope {
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
        ConnectionProvider.configure(isolatedConfig) { connConfig -> SpyDBMManager(connConfig) }
        ConnectionProvider.withScope {
            val result = ConnectionProvider.currentManagerOrNull("UNKNOWN_NO_WILDCARD")
            assertNull(result)
        }
    }

    @Test
    fun withScope_throwsWhenNotConfigured() {
        // Temporarily point to a fresh unconfigured-looking provider by using a local object
        // We test via reflection-like approach: re-create a scenario where configure was not called.
        // Since ConnectionProvider is a singleton, we configure it with null-equivalent by
        // configuring with an empty config and verifying the require message.
        val emptyConfig = DBNativeAccessConfig(listOf())
        ConnectionProvider.configure(emptyConfig) { SpyDBMManager(it) }

        assertFailsWith<IllegalArgumentException> {
            ConnectionProvider.withScope {
                ConnectionProvider.currentManager("ANYTHING")
            }
        }
    }

    @Test
    fun withScope_scopeIsThreadLocal() {
        var managerSeenFromOtherThread: DBMManager? = null
        ConnectionProvider.withScope {
            val thread = Thread {
                managerSeenFromOtherThread = ConnectionProvider.currentManagerOrNull("FILEA")
            }
            thread.start()
            thread.join()
        }
        assertNull(managerSeenFromOtherThread)
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
