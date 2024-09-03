package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import kotlin.test.Test
import kotlin.test.fail

private val MOCK_METADATA = FileMetadata(
    name = "mock",
    tableName = "mock",
    fields = listOf(
        Field("field1"),
        Field("field2"),
    ),
    fileKeys = listOf("field1")
)

/**
 * Mock implementation of DBManagerBaseImpl
 */
class MockDBManager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl() {

    override fun validateConfig() {
        connectionConfig.url.startsWith("test:")
    }

    override fun close() {
    }

    override fun openFile(name: String) = MockDBFile()

    override fun closeFile(name: String) {
    }
}

/**
 * Mock implementation of DBFile.
 * All methods are not implemented.
 */
class MockDBFile(override var name: String, override var fileMetadata: FileMetadata, override var logger: Logger?) :
    DBFile {

    constructor() : this(
        "mock",
        MOCK_METADATA,
        null
    )

    override fun eof(): Boolean {
        TODO("Not yet implemented")
    }

    override fun equal(): Boolean {
        TODO("Not yet implemented")
    }

    override fun setll(key: String): Boolean {
        TODO("Not yet implemented")
    }

    override fun setll(keys: List<String>): Boolean {
        TODO("Not yet implemented")
    }

    override fun setgt(key: String): Boolean {
        TODO("Not yet implemented")
    }

    override fun setgt(keys: List<String>): Boolean {
        TODO("Not yet implemented")
    }

    override fun chain(key: String): Result {
        TODO("Not yet implemented")
    }

    override fun chain(keys: List<String>): Result {
        TODO("Not yet implemented")
    }

    override fun read(): Result {
        TODO("Not yet implemented")
    }

    override fun readPrevious(): Result {
        TODO("Not yet implemented")
    }

    override fun readEqual(): Result {
        TODO("Not yet implemented")
    }

    override fun readEqual(key: String): Result {
        TODO("Not yet implemented")
    }

    override fun readEqual(keys: List<String>): Result {
        TODO("Not yet implemented")
    }

    override fun readPreviousEqual(): Result {
        TODO("Not yet implemented")
    }

    override fun readPreviousEqual(key: String): Result {
        TODO("Not yet implemented")
    }

    override fun readPreviousEqual(keys: List<String>): Result {
        TODO("Not yet implemented")
    }

    override fun write(record: Record): Result {
        TODO("Not yet implemented")
    }

    override fun update(record: Record): Result {
        TODO("Not yet implemented")
    }

    override fun delete(record: Record): Result {
        TODO("Not yet implemented")
    }
}

class MockDBMManagerTest {

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