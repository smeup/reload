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
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
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

        val nativeAccessConfig =
            DBNativeAccessConfig(connectionsConfig = listOf(connectionConfig))
        DBFileFactory(nativeAccessConfig).use { dbFileFactory ->
            val dbFile = dbFileFactory.open(fileName = "mock", fileMetadata = MOCK_METADATA)
            var eof = true
            kotlin.runCatching {
                eof = dbFile.eof()
            }.onSuccess {
                assertFalse(eof)
            }.onFailure {
                assert(it is NotImplementedError)
            }
        }
    }

    @Test
    fun testDifferentResultsForDifferentFiles() {

        val metadata = FileMetadata(
            name = "mock2",
            tableName = "mock2",
            fields = listOf(
                Field("foo"),
                Field("bar"),
                Field("M240DATA"),
            ),
            fileKeys = listOf("foo")
        )

        val nativeAccessConfig =
            DBNativeAccessConfig(connectionsConfig = listOf(connectionConfig))
        DBFileFactory(nativeAccessConfig).use { dbFileFactory ->
            val dbFile = dbFileFactory.open(fileName = "mock", fileMetadata = MOCK_METADATA)
            val dbFile2 = dbFileFactory.open(fileName = "mock2", fileMetadata = metadata)

            var result: Result
            var result2: Result

            result = dbFile.chain("smth")
            result2 = dbFile2.chain("smth")

            assertNotEquals(result.record.keys, result2.record.keys)

        }
    }

    @Test
    fun testDifferentResultsForSequentialCalls() {

        val metadata = FileMetadata(
            name = "mock2",
            tableName = "mock2",
            fields = listOf(
                Field("foo"),
                Field("bar"),
            ),
            fileKeys = listOf("foo")
        )

        val nativeAccessConfig =
            DBNativeAccessConfig(connectionsConfig = listOf(connectionConfig))
        DBFileFactory(nativeAccessConfig).use { dbFileFactory ->
            val dbFile = dbFileFactory.open(fileName = "mock2", fileMetadata = metadata)

            val result: Result = dbFile.chain("smth")
            val result2: Result = dbFile.chain("x")

            assertNotEquals(result.record.values, result2.record.values)
            assertNotEquals(result.record["M240DATA"], result2.record["M240DATA"])


        }
    }


}