package com.smeup.dbnative.mock

import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.FileMetadata

/**
 * Mock implementation of DBFile.
 * All methods are not implemented.
 */
class MockDBFile(override var name: String, override var fileMetadata: FileMetadata, override var logger: Logger?) :
    DBFile {

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
