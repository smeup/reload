package com.smeup.dbnative.jt400

import com.ibm.as400.access.AS400Exception
import com.ibm.as400.access.KeyedFile
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.model.FileMetadata

class JT400DBFile(override var name: String, override var fileMetadata: FileMetadata, var file: KeyedFile) : DBFile {

    //TODO rivedere
    fun positionCursorBefore(keys: List<RecordField>): Boolean {
        try {
            file.positionCursorBefore(keys2Array(keys))
            return true
        } catch (e: AS400Exception) {
            handleAS400Error(e);
            return false;
        }
    }

    /** SETLL (Set Lower Limit)
     * The SETLL operation positions a file at the next record that has a key or relative record number that is greater than or equal to the search argument (key or relative record number)
     */
    override fun setll(key: String): Boolean {
        try {
            file.positionCursor(arrayOf(key), KeyedFile.KEY_GE)
            return true
        } catch (e: AS400Exception) {
            handleAS400Error(e);
            return false;
        }
    }

    override fun setll(keys: List<RecordField>): Boolean {
        try {
            file.positionCursor(keys2Array(keys), KeyedFile.KEY_GE)
            return true
        } catch (e: AS400Exception) {
            handleAS400Error(e);
            return false;
        }
    }

    /** SETGT (Set Greater Than)
     * The SETGT operation positions a file at the next record with a key or relative record number that is greater than the key or relative record number specified
     */
    override fun setgt(key: String): Boolean {
        try {
            file.positionCursor(arrayOf(key), KeyedFile.KEY_GT)
            return true
        } catch (e: AS400Exception) {
            handleAS400Error(e);
            return false;
        }
    }

    override fun setgt(keys: List<RecordField>): Boolean {
        try {
            file.positionCursor(keys2Array(keys), KeyedFile.KEY_GT)
            return true
        } catch (e: AS400Exception) {
            handleAS400Error(e);
            return false;
        }
    }

    /** CHAIN (Random Retrieval from a File)
     * The CHAIN operation retrieves a record from a full procedural file, sets a record identifying indicator on (if specified on the input specifications), and places the data from the record into the input fields.
     */
    override fun chain(key: String): Result {
        //TODO("Attenzione alla gestione del lock")
        try {
            file.positionCursor(arrayOf(key), KeyedFile.KEY_EQ)
            return read()
        } catch (e: AS400Exception) {
            handleAS400Error(e);
            return Result(Record())
        }
    }

    override fun chain(keys: List<RecordField>): Result {
        //TODO("Attenzione alla gestione del lock")
        try {
            file.positionCursor(keys2Array(keys), KeyedFile.KEY_EQ)
            return read()
        } catch (e: AS400Exception) {
            handleAS400Error(e);
            return Result(Record())
        }
    }

    /** READ (Read a Record)
     * The READ operation reads the record, currently pointed to, from a full procedural file.
     */
    override fun read(): Result {
        //https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/rzasd/zzread.htm
        var r : Result? = null
        try {
            r =  Result(as400RecordToSmeUPRecord(file.read()))
            file.positionCursorToNext();
        } catch (e: AS400Exception) {
            handleAS400Error(e);
        }
        return r ?: fail("Read failed");
    }

    /** READP (Read Prior Record)
     * The READP operation reads the prior record from a full procedural file.
     */
    override fun readPrevious(): Result {
        var r : Result? = null
        try {
            r =  Result(as400RecordToSmeUPRecord(file.read()))
            file.positionCursorToPrevious();
        } catch (e: AS400Exception) {
            handleAS400Error(e);
        }
        return r ?: fail("Read failed");
    }

    /** READE (Read Equal Key)
     * The READE operation retrieves the next sequential record from a full procedural file if the key of the record matches the search argument.
     */
    override fun readEqual(): Result {
        return Result(as400RecordToSmeUPRecord(file.readNextEqual()))
    }

    override fun readEqual(key: String): Result {
        return Result(as400RecordToSmeUPRecord(file.readNextEqual(arrayOf(key))))
    }

    override fun readEqual(keys: List<RecordField>): Result {
        //https://code400.com/forum/forum/iseries-programming-languages/java/8386-noobie-question
        return Result(as400RecordToSmeUPRecord(file.readNextEqual(keys2Array(keys))))
    }

    /** READPE (Read Prior Equal)
     * The READPE operation retrieves the next prior sequential record from a full procedural file if the key of the record matches the search argument.
     */
    override fun readPreviousEqual(): Result {
        return Result(as400RecordToSmeUPRecord(file.readPreviousEqual()))
    }

    override fun readPreviousEqual(key: String): Result {
        return Result(as400RecordToSmeUPRecord(file.readPreviousEqual(arrayOf(key))))
    }

    override fun readPreviousEqual(keys: List<RecordField>): Result {
        return Result(as400RecordToSmeUPRecord(file.readPreviousEqual(keys2Array(keys))))
    }

    /** WRITE (Create New Records)
     * The WRITE operation writes a new record to a file.
     */
    override fun write(record: Record): Result {
        file.write(smeUPRecordToAS400Record(record))
        return Result(record)
    }

    /** UPDATE (Modify Existing Record)
     * The UPDATE operation modifies the last locked record retrieved for processing from an update disk file or subfile.
     */
    override fun update(record: Record): Result {
        file.update(smeUPRecordToAS400Record(record))
        return Result(record)
    }

    /** DELETE (Delete Record)
     * The DELETE operation deletes a record from a database file.
     */
    override fun delete(record: Record): Result {
        file.deleteRecord(recordKeys(record))
        return Result(record)
    }

    /**
     * --- UTILS ---
     */

    /**
     * https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/dbp/rbafodtrmsgmon.htm
     * If a message is sent to your program when your program is processing a database file member, the position in the file is not lost.
     * The position remains at the record it was positioned to before the message was sent, except:
     * After an end-of-file condition is reached and a message is sent to the program, the file is positioned at *START or *END.
     * After a conversion mapping message on a read operation, the file is positioned to the record containing the data that caused the message.
     */
    private fun handleAS400Error(e: AS400Exception) {
        //CPF5001 	End of file reached
        //CPF5006 	Record not found
        if (e.aS400Message != null
            //&& "CPF5006".equals(e.aS400Message.id, true)) {
            && e.aS400Message.id != null
            && e.aS400Message.id.startsWith("CPF", ignoreCase = true)) {
            return;
        }
        throw RuntimeException()
    }

    private fun fail(message: String): Nothing {
        throw RuntimeException(message)
    }

    private fun keys2Array(keys: List<RecordField>): Array<Any> {
        val keysValues = keys.map { it.value }.toTypedArray()
        /*
        val keysValues = keys.map { it.value } //.toTypedArray()
        val keysAsObj : Array<Any?> = Array(keysValues.size) {}
        for (i in 0 until keysValues.size) {
            keysValues.get(i).also { keysAsObj[i] = it }
        }
        //java.lang.Object y = Object
        */
        return keysValues;
    }
    private fun recordKeys(record: Record): Array<Any> {
        var keysValues = mutableListOf<Any>()
        for (key in fileMetadata.fileKeys) {
            if (record[key] != null) {
                keysValues.add(record[key]!!)
            }
        }
        return keysValues.toTypedArray()
    }

    //fun com.ibm.as400.access.Record?.currentRecordToValues(): Record {
    private fun as400RecordToSmeUPRecord(r: com.ibm.as400.access.Record?): Record {
        // TODO create a unit test for the isAfterLast condition
        if (r == null) { //TODO || this.isAfterLast
            return Record()
        }
        val result = Record()
        val fields = file.recordFormat.fieldNames
        for (name in fields) {
            val value = r.getField(name)
            result.add(RecordField(name, value))
        }
        return result
    }

    //fun com.ibm.as400.access.Record?.currentRecordToValues(): Record {
    private fun smeUPRecordToAS400Record(r: Record?): com.ibm.as400.access.Record? {
        if (r == null) {
            return null //com.ibm.as400.access.Record()
        }
        val result = com.ibm.as400.access.Record()
        result.recordFormat = file.recordFormat
        for ((name, value) in r) {
            if (value is Int) {
                result.setField(name, value.toBigDecimal())
            } else {
                result.setField(name, value)
            }
        }
        /*
        val fields = file.recordFormat.fieldNames
        for (name in fields) {
            result.setField(name, r.getValue(name))
        }
         */
        return result
    }


}