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

package com.smeup.dbnative.sql

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.sql.utils.*
import com.smeup.dbnative.utils.TypedMetadata
import com.smeup.dbnative.utils.fieldByType
import org.junit.AfterClass
import org.junit.BeforeClass
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * READ/READP right after a CHAIN, and [com.smeup.dbnative.file.DBFile.resetPosition].
 * IBM i semantics followed (proved on IBM i V7R5M0, docs/ibmi-proof RPGPROOF5 in the smeuperp workspace): after a
 * successful CHAIN, READ/READE return the record that follows the chained one (also the next DUPLICATE of the key) and
 * READP the one that precedes it; a CHAIN that finds nothing leaves the position unchanged; after a CLOSE/OPEN the
 * first READ returns the first record.
 */
class SQLReadAfterChainTest {

    companion object {

        private lateinit var dbManager: SQLDBMManager

        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateEmployeeTable(dbManager)
            createDuplicateKeysTable()
        }

        /** Same data as the IBM i proof RPGPROOF5: 010/A 020/A 030/A 030/B 040/A, key KK, duplicates allowed. */
        private fun createDuplicateKeysTable() {
            val fields = listOf("KK" fieldByType CharacterType(3), "VV" fieldByType CharacterType(3))
            // created without key (so no UNIQUE constraint), then registered as keyed on KK
            createFile(TypedMetadata(DUP_FILE, DUP_FILE, fields, emptyList()), dbManager)
            dbManager.registerMetadata(FileMetadata(DUP_FILE, DUP_FILE, fields.map { it.field }, listOf("KK")), true)
            val dbFile = dbManager.openFile(DUP_FILE)
            listOf("010" to "A", "020" to "A", "030" to "A", "030" to "B", "040" to "A").forEach { (k, v) ->
                dbFile.write(Record(RecordField("KK", k), RecordField("VV", v)))
            }
            dbManager.closeFile(DUP_FILE)
        }

        private const val DUP_FILE = "DUPKEYF"

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dbManager.close()
            destroyDatabase()
        }
    }

    private fun empNo(record: com.smeup.dbnative.file.Record) = record["EMPNO"].toString().trim()

    @Test
    fun readAfterChainReturnsTheFollowingRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        assertEquals("000030", empNo(dbFile.chain("000030").record))
        assertEquals("000050", empNo(dbFile.read().record))
        assertEquals("000060", empNo(dbFile.read().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun readPreviousAfterChainReturnsThePrecedingRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        assertEquals("000030", empNo(dbFile.chain("000030").record))
        assertEquals("000020", empNo(dbFile.readPrevious().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun chainAfterChainStillWorks() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        dbFile.chain("000030")
        assertEquals("000050", empNo(dbFile.chain("000050").record))
        assertEquals("000060", empNo(dbFile.read().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun resetPositionAfterChainMakesReadStartFromTheFirstRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        dbFile.chain("000030")
        dbFile.resetPosition()
        assertEquals("000010", empNo(dbFile.read().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun resetPositionAfterReadMakesReadStartFromTheFirstRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        dbFile.read()
        dbFile.read()
        dbFile.resetPosition()
        assertEquals("000010", empNo(dbFile.read().record))
        assertTrue(!dbFile.eof())
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    private fun kv(record: Record) = if (record.isEmpty()) "-" else record["KK"].toString().trim() + "/" + record["VV"].toString().trim()

    @Test
    fun readAfterChainReturnsTheNextDuplicate() {
        val dbFile = dbManager.openFile(DUP_FILE)
        assertEquals("030/A", kv(dbFile.chain("030").record))
        assertEquals("030/B", kv(dbFile.read().record))
        assertEquals("040/A", kv(dbFile.read().record))
        dbManager.closeFile(DUP_FILE)
    }

    @Test
    fun readAfterChainOfASingleKeyReturnsTheFirstDuplicateOfTheNext() {
        val dbFile = dbManager.openFile(DUP_FILE)
        dbFile.chain("020")
        assertEquals("030/A", kv(dbFile.read().record))
        assertEquals("030/B", kv(dbFile.read().record))
        dbManager.closeFile(DUP_FILE)
    }

    @Test
    fun readPreviousAfterChainReturnsThePreviousDuplicate() {
        val dbFile = dbManager.openFile(DUP_FILE)
        assertEquals("040/A", kv(dbFile.chain("040").record))
        assertEquals("030/B", kv(dbFile.readPrevious().record))
        assertEquals("030/A", kv(dbFile.readPrevious().record))
        assertEquals("020/A", kv(dbFile.readPrevious().record))
        dbManager.closeFile(DUP_FILE)
    }

    @Test
    fun readEqualAfterChainReturnsTheNextDuplicate() {
        val dbFile = dbManager.openFile(DUP_FILE)
        assertEquals("030/A", kv(dbFile.chain("030").record))
        assertEquals("030/B", kv(dbFile.readEqual("030").record))
        assertTrue(dbFile.readEqual("030").record.isEmpty())
        dbManager.closeFile(DUP_FILE)
    }

    @Test
    fun chainNotFoundLeavesThePositionUnchanged() {
        // IBM i proof T6+T7: after READE 030 returned 030/B, a CHAIN of a missing key and a READ give 040/A
        val dbFile = dbManager.openFile(DUP_FILE)
        assertEquals("030/A", kv(dbFile.chain("030").record))
        assertEquals("030/B", kv(dbFile.readEqual("030").record))
        assertTrue(dbFile.chain("999").record.isEmpty())
        assertEquals("040/A", kv(dbFile.read().record))
        dbManager.closeFile(DUP_FILE)
    }

    @Test
    fun chainNotFoundAfterSetllKeepsThePositioning() {
        val dbFile = dbManager.openFile(DUP_FILE)
        dbFile.setll("020")
        assertTrue(dbFile.chain("999").record.isEmpty())
        assertEquals("020/A", kv(dbFile.read().record))
        dbManager.closeFile(DUP_FILE)
    }

    @Test
    fun chainNotFoundOnAFreshFileThenReadStartsFromTheFirstRecord() {
        val dbFile = dbManager.openFile(DUP_FILE)
        assertTrue(dbFile.chain("999").record.isEmpty())
        assertEquals("010/A", kv(dbFile.read().record))
        dbManager.closeFile(DUP_FILE)
    }
}
