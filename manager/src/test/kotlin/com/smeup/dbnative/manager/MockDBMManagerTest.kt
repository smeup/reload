package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import kotlin.test.*


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
    // - with the class: protocol (url = "class:")
    // - with any user and password
    // - will be managed by the MockDBManager class specified in url
    val connectionConfig = ConnectionConfig(
        fileName = "*",
        url = "class:com.smeup.dbnative.mock.MockDBManager",
        user = "*",
        password = "*"
    )

    /***
     * Test that a given impl always overrides the used one
     */
    @Test
    fun testMockProtocolImpl() {
        val connectionConfig = ConnectionConfig(
            fileName = "*",
            url = "class:com.smeup.dbnative.sql.SQLDBMManager", //unused
            user = "*",
            password = "*",
            impl = "com.smeup.dbnative.mock.MockDBManager"
        )
        val nativeAccessConfig = DBNativeAccessConfig(connectionsConfig = listOf(connectionConfig))
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

    /***
     * Test that the class: protocol is handled by passing a custom implementation in the ConnectionConfig.url
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


        }
    }

    @Test
    fun testSequentialCalls() {

        val metadata = FileMetadata(
            name = "MU24020F",
            tableName = "MU24020F",
            fields = listOf(
                Field("foo"),
                Field("bar"),
            ),
            fileKeys = listOf("foo")
        )

        val nativeAccessConfig =
            DBNativeAccessConfig(connectionsConfig = listOf(connectionConfig))
        DBFileFactory(nativeAccessConfig).use { dbFileFactory ->
            val dbFile = dbFileFactory.open(fileName = "MU24020F", fileMetadata = metadata)

            var resultList = ArrayList<Result>()
            while (!dbFile.eof()) {
                var res = dbFile.chain("aa")
//                println(res.record.get("M240DATA"))
                resultList.add(res)
            }
            println(resultList.size)
           println(resultList.get(0).record.get("M240DATA"))
           println(resultList.get(522129).record.get("M240DATA"))
           println(resultList.get(522130).record.get("M240DATA"))
           println(resultList.get(522131).record.get("M240DATA"))


        }
    }


}