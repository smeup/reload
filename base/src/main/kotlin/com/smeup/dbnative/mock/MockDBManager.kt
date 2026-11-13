package com.smeup.dbnative.mock

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.model.copyForOpen

/**
 * Mock implementation of DBManagerBaseImpl
 */
class MockDBManager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl<Nothing, Nothing>() {

    override fun validateConfig() {
    }

    override fun close() {
    }

    override fun openFile(name: String) = MockDBFile(name, metadataOf(name), null)

    override fun openFile(name: String, metadata: FileMetadata) = MockDBFile(name, metadata.copyForOpen(name), null)

    override fun closeFile(name: String) {
    }
}