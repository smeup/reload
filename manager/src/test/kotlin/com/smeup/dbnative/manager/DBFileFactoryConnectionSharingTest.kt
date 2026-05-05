package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.sql.SQLDBMManager
import com.smeup.dbnative.sql.toDataSource
import com.smeup.dbnative.utils.TypedField
import com.smeup.dbnative.utils.fieldByType
import com.smeup.dbnative.utils.fieldList
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

private val SHARING_CONNECTION_CONFIG = ConnectionConfig(
    fileName = "*",
    url = "jdbc:hsqldb:mem:SHARING_TEST",
    user = "sa",
    password = "root"
)

private val SHARING_ACCESS_CONFIG = DBNativeAccessConfig(listOf(SHARING_CONNECTION_CONFIG))

private val TABLE_METADATA = FileMetadata(
    "SHARE1L",
    "SHARE1F",
    listOf<TypedField>("NAME" fieldByType CharacterType(20)).fieldList(),
    listOf("NAME")
)

/**
 * Verifies that [DBFileFactory] and [ConnectionProvider] share scoped SQL connections.
 */
class DBFileFactoryConnectionSharingTest {

    private lateinit var bootstrapManager: SQLDBMManager

    @Before
    fun setUp() {
        bootstrapManager = SQLDBMManager(SHARING_CONNECTION_CONFIG)
        bootstrapManager.connection.createStatement().use {
            it.executeUpdate("CREATE TABLE IF NOT EXISTS SHARE1F (NAME CHAR(20))")
        }
        bootstrapManager.registerMetadata(TABLE_METADATA, true)
        ConnectionProvider.configure(SHARING_ACCESS_CONFIG, null)
    }

    @After
    fun tearDown() {
        bootstrapManager.runCatching {
            connection.createStatement().use { it.executeUpdate("DROP TABLE IF EXISTS SHARE1F") }
        }
        bootstrapManager.close()
        bootstrapManager.unregisterMetadata("SHARE1L")
    }

    @Test
    fun open_insideScope_reusesManagerFromScope() {
        val factory = DBFileFactory(SHARING_ACCESS_CONFIG)
        var scopedManager: com.smeup.dbnative.DBMManager? = null
        ConnectionProvider.withScope {
            scopedManager = ConnectionProvider.currentManager("SHARE1L")
            val dbFile = factory.open("SHARE1L", null)
            assertNotNull(dbFile)
            dbFile.close()
        }
        factory.close()
    }

    @Test
    fun open_outsideScope_createsOwnManager() {
        val factory = DBFileFactory(SHARING_ACCESS_CONFIG)
        val result = ConnectionProvider.currentManagerOrNull("SHARE1L")
        assertNull(result)
        val dbFile = factory.open("SHARE1L", null)
        assertNotNull(dbFile)
        dbFile.close()
        factory.close()
    }

    @Test
    fun open_insideScope_sameConnectionAsProvider() {
        val factory = DBFileFactory(SHARING_ACCESS_CONFIG)
        ConnectionProvider.withScope {
            val scopedMgr = ConnectionProvider.currentManager("SHARE1L") as? SQLDBMManager
            assertNotNull(scopedMgr)
            val providerConn = scopedMgr.connection

            val ds = scopedMgr.toDataSource()
            assertNotNull(ds)
            val dsConn = ds.getConnection()
            assertSame(providerConn, dsConn)
        }
        factory.close()
    }
}
