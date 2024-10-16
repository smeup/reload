package com.smeup.dbnative.mock

import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.FileMetadata
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Mock implementation of DBFile.
 * All methods are not implemented.
 */
class MockDBFile(override var name: String, override var fileMetadata: FileMetadata, override var logger: Logger?) :
    DBFile {

    val maxMockRecords = 522131
    var returnedRecords = 0
    var mockedResult: Result

    init {
        val recordFields = fileMetadata.fields.map { field ->
            RecordField(field.name, if (field.numeric) "0" else if (field.text.isBlank())  field.name else field.text)
        }
        val extraFields = listOf(
            RecordField("M240ORE", "0"),
            RecordField("M240KM", "0"),
            RecordField("M240KMSE", "0"),
            RecordField("M240DATA", "00010101"),
            RecordField("E§CINC", "42")
        )

        val allFields = (recordFields + extraFields)

        val record = Record(*allFields.toTypedArray())

        mockedResult = Result(record = record)
    }

    private fun nextIteration(): Result {
        returnedRecords++
        val previousData = mockedResult.record["M240DATA"] as String

        val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        val previousDate = LocalDate.parse(previousData, formatter)
        val nextDate = previousDate.plusDays(1)

        val recordFields = fileMetadata.fields.map { field ->
            RecordField(field.name, if (field.numeric) "0" else if (field.text.isBlank())  field.name else field.text)
        }
        val extraFields = listOf(
            RecordField("M240ORE", "0"),
            RecordField("M240KM", "0"),
            RecordField("M240KMSE", "0"),
            RecordField("M240DATA", nextDate.format(formatter)),
            RecordField("E§CINC", "42")
        )

        val allFields = (recordFields + extraFields)

        val record = Record(*allFields.toTypedArray())

        mockedResult = Result(record = record)

        return mockedResult
    }




    override fun eof(): Boolean {
        return returnedRecords >= maxMockRecords
    }

    override fun equal(): Boolean {
        return true
    }

    override fun setll(key: String): Boolean {
        return true
    }

    override fun setll(keys: List<String>): Boolean {
        return true
    }

    override fun setgt(key: String): Boolean {
        return true
    }

    override fun setgt(keys: List<String>): Boolean {
        return true
    }

    override fun chain(key: String): Result {
        return nextIteration()
    }

    override fun chain(keys: List<String>): Result {
        return nextIteration()
    }

    override fun read(): Result {
        return nextIteration()

    }

    override fun readPrevious(): Result {
        return nextIteration()

    }

    override fun readEqual(): Result {
        return nextIteration()

    }

    override fun readEqual(key: String): Result {
        return nextIteration()

    }

    override fun readEqual(keys: List<String>): Result {
        return nextIteration()

    }

    override fun readPreviousEqual(): Result {
        return nextIteration()

    }

    override fun readPreviousEqual(key: String): Result {
        return nextIteration()

    }

    override fun readPreviousEqual(keys: List<String>): Result {
        return nextIteration()

    }

    override fun write(record: Record): Result {
        return mockedResult
    }

    override fun update(record: Record): Result {
        return mockedResult
    }

    override fun delete(record: Record): Result {
        return mockedResult
    }
}
