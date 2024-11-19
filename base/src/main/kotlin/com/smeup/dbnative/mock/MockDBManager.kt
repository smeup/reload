package com.smeup.dbnative.mock

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl

/**
 * Mock implementation of DBManagerBaseImpl
 */
class MockDBManager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl() {

    override fun validateConfig() {
    }

    override fun close() {
    }

    override fun openFile(name: String): MockDBFile {
        // Ensure file exists in database
        require(this.existFile(name)) { "File '$name' do not exists in database." }
        // Ensure metadata is not null
        val fileMetadata = metadataOf(name)
        require(fileMetadata != null) { "Metadata for file '$name' is null." }

        return MockDBFile(name, fileMetadata, null);
    }



    override fun closeFile(name: String) {
    }
}