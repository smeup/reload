package com.smeup.dbnative.mock

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata

/**
 * Mock implementation of DBManagerBaseImpl
 */
class MockDBManager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl() {

    private val MOCK_METADATA = FileMetadata(
        name = "mock",
        tableName = "mock",
        fields = listOf(
            Field("field1"),
            Field("field2"),
        ),
        fileKeys = listOf("field1")
    )

    override fun validateConfig() {
    }

    override fun close() {
    }

    override fun openFile(name: String) = MockDBFile(name, MOCK_METADATA, null)

    override fun closeFile(name: String) {
    }
}