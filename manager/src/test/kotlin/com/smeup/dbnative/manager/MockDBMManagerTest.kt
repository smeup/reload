package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.mock.MockDBManager
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import kotlin.test.Test
import kotlin.test.fail


class MockDBMManagerTest {

    private val MOCK_METADATA = FileMetadata(
        name = "mock",
        tableName = "mock",
        fields = listOf(
            Field("field1"),
            Field("field2"),
        ),
        fileKeys = listOf("field1")
    )

    /***
     * Test that the mock: protocol is not handled for default
     */
    @Test
    fun testMockProtocolNotHandled() {
        val connectionConfig = ConnectionConfig(
            fileName = "*",
            url = "mock:",
            user = "*",
            password = "*"
        )
        val nativeAccessConfig = DBNativeAccessConfig(connectionsConfig = listOf(connectionConfig))
        DBFileFactory(nativeAccessConfig).use { dbFileFactory ->
            runCatching {
                dbFileFactory.open(fileName = "mock", fileMetadata = MOCK_METADATA)
            }.onSuccess {
                fail("Should not be able to open a file because the mock: protocol is not handled")
            }.onFailure {
                assert(it is IllegalArgumentException)
                assert(it.message == "mock: not handled")
            }
        }
    }

    /***
     * Test that the mock: protocol is handled by passing a custom implementation in the ConnectionConfig.impl
     * property
     */
    @Test
    fun testMockProtocolHandled() {
        // I say that
        // - all the files (fileName = "*")
        // - with the mock: protocol (url = "mock:")
        // - with any user and password
        // - will be managed by the MockDBManager class
        val connectionConfig = ConnectionConfig(
            fileName = "*",
            url = "mock:",
            user = "*",
            password = "*",
            impl = MockDBManager::class.java.name
        )
        val nativeAccessConfig =
            DBNativeAccessConfig(connectionsConfig = listOf(connectionConfig))
        DBFileFactory(nativeAccessConfig).use { dbFileFactory ->
            val dbFile = dbFileFactory.open(fileName = "mock", fileMetadata = MOCK_METADATA)
            kotlin.runCatching {
                dbFile.eof()
            }.onSuccess {
                fail("Should not be able to call eof")
            }.onFailure {
                assert(it is NotImplementedError)
            }
        }
    }

}