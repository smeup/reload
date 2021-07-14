/*
 * Copyright 2020 The Reload project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.smeup.dbnative.jt400

import com.ibm.as400.access.AS400DataType
import com.ibm.as400.access.AS400Exception
import com.ibm.as400.access.KeyedFile
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.FileMetadata
import java.math.BigDecimal

private enum class CursorAction {
    NONE, SETLL, SETGT
}

class JT400DBFile(override var name: String,
                  override var fileMetadata: FileMetadata,
                  var file: KeyedFile,
                  override var logger: Logger? = null) : DBFile {

    private var equalFlag: Boolean = false
    private var eofReached: Boolean = false
    private var previousAction: CursorAction = CursorAction.NONE

    private fun resetStatus() {
        this.equalFlag = false
        this.eofReached = false
    }

    override fun eof(): Boolean {
        /*
        try {
            file.positionCursorToNext()
            file.positionCursorToPrevious()
        } catch (e: AS400Exception) {
            val id = as400ErrorID(e);
            //CPF5001 	End of file reached
            if (as400ErrorID(e).startsWith("CPF5001", ignoreCase = true)) {
                return true
            }
        }
        return false
         */
        return eofReached
    }

    override fun equal(): Boolean {
        /*
        E' una cosa più complessa: vuol dire che quando fai una SETLL con più chiavi EQUAL vale true se il puntatore
        si posizione prima dell'elemento che soddisfa esattamente le chiavi, false se no.
        Esempio: certo nella tabella comuni il comune di Erbusco con chiavi ITALIA-LOMBARDIA-ERBUSCO
        Se il record viene trovato il puntatore viene posto prima del record e EQUAL viene posto a true.
        Se invece Il record non viene trovato, il puntatore viene posto al primo record successivo a quello che si cercava
        (ad esempio, ITALIA-LOMBARDIA-ESINE) ma con il flag EQUAL spento.
        Praticamente ll DB ti torna comunque il puntatore al record ma con diverso significato a seconda del flag EQUAL
        Vale solo per SETLL e SETGT, per il CHAIN c'è una cosa simile ma si chiama found()
        Si, che si accende o meno a seconda di come è andata la setll o la setgt precedente
        Per me nel tuo caso è una informazione tornata in qualche modo dai comandi del JTOpen
        Mi aspetterei che quando fai un callSetLL la libreria ti torna il flag
         */
        return equalFlag
    }

    /** SETLL (Set Lower Limit)
     * The SETLL operation positions a file at the next record that has a key or relative record number that is greater than or equal to the search argument (key or relative record number)
     */
    override fun setll(key: String): Boolean {
        return setll(listOf(key))
    }

    override fun setll(keys: List<String>): Boolean {
        this.previousAction = CursorAction.SETLL
        resetStatus()
        try {
            file.positionCursor(keys2Array(keys), KeyedFile.KEY_EQ)
            this.equalFlag = true
        } catch (e: AS400Exception) {
        }
        try {
            //file.positionCursorBefore(keys2Array(keys))
            file.positionCursor(keys2Array(keys), KeyedFile.KEY_LT)
            //file.positionCursor(keys2Array(keys), KeyedFile.KEY_LT)
            return true
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            file.positionCursorBeforeFirst()
            return true
        }
    }

    /** SETGT (Set Greater Than)
     * The SETGT operation positions a file at the next record with a key or relative record number that is greater than the key or relative record number specified
     */
    override fun setgt(key: String): Boolean {
        return setgt(listOf(key))
    }

    override fun setgt(keys: List<String>): Boolean {
        this.previousAction = CursorAction.SETGT
        resetStatus()
        try {
            file.positionCursor(keys2Array(keys), KeyedFile.KEY_EQ)
            this.equalFlag = true
        } catch (e: AS400Exception) {
        }
        try {
            file.positionCursor(keys2Array(keys), KeyedFile.KEY_GT)
            return true
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            file.positionCursorAfterLast()
            return true
        }
    }

    /** CHAIN (Random Retrieval from a File)
     * The CHAIN operation retrieves a record from a full procedural file, sets a record identifying indicator on (if specified on the input specifications), and places the data from the record into the input fields.
     */
    override fun chain(key: String): Result {
        return chain(listOf(key))
    }

    override fun chain(keys: List<String>): Result {
        this.previousAction = CursorAction.NONE
        resetStatus()
        //TODO("Attenzione alla gestione del lock")
        try {
            /*
            file.positionCursor(keys2Array(keys), KeyedFile.KEY_EQ)
            var r : Result? = Result(as400RecordToSmeUPRecord(file.read()))
            //file.positionCursorToNext();
            return r ?: fail("Read failed");
            */
            return Result(as400RecordToSmeUPRecord(file.read(keys2Array(keys))))
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            return Result(Record())
        }
    }

    /** READ (Read a Record)
     * The READ operation reads the record, currently pointed to, from a full procedural file.
     */
    override fun read(): Result {
        //https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/rzasd/zzread.htm
        var r : Result
        try {
            if (this.previousAction==CursorAction.SETLL) {
                file.positionCursorToNext()
            }
            r =  Result(as400RecordToSmeUPRecord(file.read()))
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            r = Result(Record())
        }
        try {
            file.positionCursorToNext()
        } catch (e: AS400Exception) {
            handleAS400Error(e)
        }
        this.previousAction = CursorAction.NONE
        return r
    }

    /** READP (Read Prior Record)
     * The READP operation reads the prior record from a full procedural file.
     */
    override fun readPrevious(): Result {
        resetStatus()
        var r : Result
        try {
            if (this.previousAction!=CursorAction.SETLL) {
                file.positionCursorToPrevious()
            }
            r =  Result(as400RecordToSmeUPRecord(file.read()))
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            r = Result(Record())
        }
        this.previousAction = CursorAction.NONE
        return r
    }

    /** READE (Read Equal Key)
     * The READE operation retrieves the next sequential record from a full procedural file if the key of the record matches the search argument.
     */
    override fun readEqual(): Result {
        resetStatus()
        return try {
            val r = file.readNextEqual()
            this.eofReached = r == null
            Result(as400RecordToSmeUPRecord(r))
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            Result(Record())
        } finally {
            this.previousAction = CursorAction.NONE
        }
    }

    override fun readEqual(key: String): Result {
        return readEqual(listOf(key))
    }

    override fun readEqual(keys: List<String>): Result {
        resetStatus()
        //https://code400.com/forum/forum/iseries-programming-languages/java/8386-noobie-question
        return try {
            //val r = if (this.previousAction==CursorAction.SETGT)  file.read(keys2Array(keys)) else file.readNextEqual(keys2Array(keys))
            if (this.previousAction==CursorAction.SETGT) {
                file.positionCursorToPrevious()
            }
            val r = file.readNextEqual(keys2Array(keys))
            this.eofReached = r == null
            Result(as400RecordToSmeUPRecord(r))
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            Result(Record())
        } finally {
            this.previousAction = CursorAction.NONE
        }
        /*
        var r : Result? = null
        try {
            r =  Result(as400RecordToSmeUPRecord(file.read(keys2Array(keys))))
            file.positionCursorToNext();
        } catch (e: AS400Exception) {
            handleAS400Error(e);
        }
        return r ?: fail("Read failed");
         */
    }

    /** READPE (Read Prior Equal)
     * The READPE operation retrieves the next prior sequential record from a full procedural file if the key of the record matches the search argument.
     */
    override fun readPreviousEqual(): Result {
        resetStatus()
        return try {
            val r = file.readPreviousEqual()
            this.eofReached = r == null
            Result(as400RecordToSmeUPRecord(r))
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            Result(Record())
        } finally {
            this.previousAction = CursorAction.NONE
        }
    }

    override fun readPreviousEqual(key: String): Result {
        return readPreviousEqual(listOf(key))
    }

    override fun readPreviousEqual(keys: List<String>): Result {
        resetStatus()
        return try {
            if (this.previousAction==CursorAction.SETLL) {
                file.positionCursorToNext()
            }
            val r = file.readPreviousEqual(keys2Array(keys))
            this.eofReached = r == null
            Result(as400RecordToSmeUPRecord(r))
        } catch (e: AS400Exception) {
            handleAS400Error(e)
            Result(Record())
        } finally {
            this.previousAction = CursorAction.NONE
        }
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
        val eid = as400ErrorID(e).toUpperCase()
        if (eid.startsWith("CPF5001")) {
            this.eofReached = true
            return
        } else if (eid.startsWith("CPF")) {
            return
        }
        throw RuntimeException()
    }
    private fun as400ErrorID(e: AS400Exception) : String {
        //CPF5001 	End of file reached
        //CPF5006 	Record not found
        if (e.aS400Message != null
            && e.aS400Message.id != null) {
            return e.aS400Message.id
        }
        return ""
    }

    private fun keys2Array(keys: List<String>): Array<Any> {
        //return keys.map { it.value }.toTypedArray()
        val keysValues = mutableListOf<Any>()
        //for (key in fileMetadata.fileKeys) {
        for (i in keys.indices) {
            val keyName = fileMetadata.fileKeys[i]
            val keyValue = keys.get(i)
            if (numericField(keyName)) {
                keysValues.add(BigDecimal(keyValue))
            } else {
                keysValues.add(keyValue)
            }
        }
        return keysValues.toTypedArray()
    }

    private fun recordKeys(record: Record): Array<Any> {
        val keysValues = mutableListOf<Any>()
        for (key in fileMetadata.fileKeys) {
            if (record[key] != null) {
                keysValues.add(record[key]!!)
            }
        }
        return keysValues.toTypedArray()
    }

    private fun as400RecordToSmeUPRecord(r: com.ibm.as400.access.Record?): Record {
        // TODO create a unit test for the isAfterLast condition
        if (r == null) { //TODO || this.isAfterLast
            return Record()
        }
        val result = Record()
        val fields = file.recordFormat.fieldNames
        for (name in fields) {
            val value = r.getField(name)
            result.add(RecordField(name, value.toString()))
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
            if (numericField(name)) {
                result.setField(name, BigDecimal(value))
            } else {
                //try {
                    result.setField(name, value.trimEnd())
                //} catch (ex : Exception) {
                //    println(ex.message)
                //}
            }
        }
        return result
    }

    private fun numericField(name : String) : Boolean {
        val dataType = file.recordFormat.getFieldDescription(name).dataType.instanceType
        //val field : Field? = this.fileMetadata.getField(name)
        //val type : FieldType? =  field?.type
        return when (dataType) {
            AS400DataType.TYPE_ZONED,
            AS400DataType.TYPE_PACKED,
            AS400DataType.TYPE_DECFLOAT,
            AS400DataType.TYPE_BIN1,
            AS400DataType.TYPE_BIN2,
            AS400DataType.TYPE_BIN4,
            AS400DataType.TYPE_BIN8,
            AS400DataType.TYPE_UBIN1,
            AS400DataType.TYPE_UBIN2,
            AS400DataType.TYPE_UBIN4,
            AS400DataType.TYPE_UBIN8,
            AS400DataType.TYPE_FLOAT4,
            AS400DataType.TYPE_FLOAT8 ->
                true
            else ->
                false
        }
    }

}