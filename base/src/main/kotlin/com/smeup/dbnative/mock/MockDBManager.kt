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

    override fun openFile(name: String) = MockDBFile(name, metadataOf(name), null)

    override fun closeFile(name: String) {
    }
}