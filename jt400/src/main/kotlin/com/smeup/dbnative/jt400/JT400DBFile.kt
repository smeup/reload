package com.smeup.dbnative.jt400

import com.ibm.as400.access.AS400Exception
import com.ibm.as400.access.KeyedFile
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.model.FileMetadata

class JT400DBFile(override var name: String, override var fileMetadata: FileMetadata, var file: KeyedFile) : DBFile {

    /** SETLL (Set Lower Limit)
     * The SETLL operation positions a file at the next record that has a key or relative record number that is greater than or equal to the search argument (key or relative record number)
     */
    override fun setll(key: String): Boolean {

        try {
            file.positionCursor(arrayOf(key), KeyedFile.KEY_GE);
            return true;
        } catch (e: AS400Exception) {
            if (e.aS400Message != null && "CPF5006".equals(e.aS400Message.id, true)) {
                //record not found
                return false;
            }
            TODO( reason = "throw error")
        }

    }

    override fun setll(keys: List<RecordField>): Boolean {
        TODO("Not yet implemented")
    }

    /** SETGT (Set Greater Than)
     * The SETGT operation positions a file at the next record with a key or relative record number that is greater than the key or relative record number specified
     */
    override fun setgt(key: String): Boolean {

        file.positionCursor(arrayOf(key), KeyedFile.KEY_GT);

    }

    override fun setgt(keys: List<RecordField>): Boolean {
        TODO("Not yet implemented")
    }

    /** CHAIN (Random Retrieval from a File)
     * The CHAIN operation retrieves a record from a full procedural file, sets a record identifying indicator on (if specified on the input specifications), and places the data from the record into the input fields.
     */
    override fun chain(key: String): Result {
        TODO("Not yet implemented")
        TODO("Attenzione alla gestione del lock")
    }

    override fun chain(keys: List<RecordField>): Result {
        TODO("Not yet implemented")
    }

    /** READ (Read a Record)
     * The READ operation reads the record, currently pointed to, from a full procedural file.
     */
    override fun read(): Result {

        file.read();

    }

    /** READP (Read Prior Record)
     * The READP operation reads the prior record from a full procedural file.
     */
    override fun readPrevious(): Result {

        file.read();

    }

    /** READE (Read Equal Key)
     * The READE operation retrieves the next sequential record from a full procedural file if the key of the record matches the search argument.
     */
    override fun readEqual(): Result {

        file.readNextEqual();

    }

    override fun readEqual(key: String): Result {
        TODO("Not yet implemented")
    }

    override fun readEqual(keys: List<RecordField>): Result {
        TODO("Not yet implemented")
    }

    /** READPE (Read Prior Equal)
     * The READPE operation retrieves the next prior sequential record from a full procedural file if the key of the record matches the search argument.
     */
    override fun readPreviousEqual(): Result {

        file.readPreviousEqual();

    }

    override fun readPreviousEqual(key: String): Result {
        TODO("Not yet implemented")
    }

    override fun readPreviousEqual(keys: List<RecordField>): Result {
        TODO("Not yet implemented")
    }

    /** WRITE (Create New Records)
     * The WRITE operation writes a new record to a file.
     */
    override fun write(record: Record): Result {
        TODO("Not yet implemented")
    }

    /** UPDATE (Modify Existing Record)
     * The UPDATE operation modifies the last locked record retrieved for processing from an update disk file or subfile.
     */
    override fun update(record: Record): Result {
        TODO("Not yet implemented")
    }

    /** DELETE (Delete Record)
     * The DELETE operation deletes a record from a database file.
     */
    override fun delete(record: Record): Result {
        TODO("Not yet implemented")
    }
}