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

import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.sql.utils.*
import com.smeup.dbnative.utils.TypedMetadata
import com.smeup.dbnative.utils.fieldByType
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Covers Relative Record Number (RRN) access on unkeyed files (Native2SQLAdapter's `rrnMode`,
 * see checkKeys()/tableExpr()): CHAIN by RRN against a file whose metadata declares no keys is
 * resolved via `ROW_NUMBER() OVER (ORDER BY ...)`, using the physical table's primary key first
 * and falling back to every field declared in the file's own metadata, per
 * [SQLDBFile]'s `rrnOrderingColumns`.
 */
class SQLRRNChainTest {

    companion object {

        private lateinit var dbManager: SQLDBMManager

        private val employeeFields = listOf(
            Field("EMPNO"), Field("FIRSTNME"), Field("MIDINIT"), Field("LASTNAME"), Field("WORKDEPT")
        )

        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateMunicipalityTable(dbManager)
            createAndPopulateEmployeeTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dbManager.close()
            destroyDatabase()
        }
    }

    @Test
    fun chainByRRNUsingPrimaryKeyOrdering() {
        // EMPLOYEE_RRN: same physical table as EMPLOYEE, but declared unkeyed - forces RRN mode,
        // ordered by EMPLOYEE's real primary key (EMPNO) since connection.primaryKeys() resolves it.
        dbManager.registerMetadata(FileMetadata("EMPLOYEE_RRN", EMPLOYEE_TABLE_NAME, employeeFields, emptyList()), true)
        val dbFile = dbManager.openFile("EMPLOYEE_RRN")
        val result = dbFile.chain(listOf("2"))
        assertEquals("000020", result.record["EMPNO"]?.trim())
        assertEquals("THOMPSON", result.record["LASTNAME"]?.trim())
        dbManager.closeFile("EMPLOYEE_RRN")
    }

    @Test
    fun chainByRRNUsingMetadataFieldsFallback() {
        // NOPKTABLE has no primary key at all: RRN falls back to ordering by every field declared
        // in the file's own metadata (CODE, DESCR), in that order - rows are inserted in a
        // deliberately different order to prove the fallback drives the order, not insertion order.
        createFile(
            TypedMetadata(
                "NOPKTABLE",
                "NOPKTABLE",
                listOf("CODE" fieldByType CharacterType(5), "DESCR" fieldByType CharacterType(20)),
                emptyList(),
            ),
            dbManager,
        )
        dbManager.execute(
            listOf(
                "INSERT INTO \"NOPKTABLE\" (CODE, DESCR) VALUES ('C', 'third')",
                "INSERT INTO \"NOPKTABLE\" (CODE, DESCR) VALUES ('A', 'first')",
                "INSERT INTO \"NOPKTABLE\" (CODE, DESCR) VALUES ('B', 'second')",
            ),
        )
        val dbFile = dbManager.openFile("NOPKTABLE")
        val result = dbFile.chain(listOf("1"))
        assertEquals("A", result.record["CODE"]?.trim())
        assertEquals("first", result.record["DESCR"]?.trim())
        dbManager.closeFile("NOPKTABLE")
    }

    @Test
    fun chainByRRNWithNoResolvableOrderingThrows() {
        // No primary key resolvable and no fields declared in metadata to fall back on: RRN
        // access must fail clearly instead of building a query with no deterministic order.
        // checkKeys() throws before any SQL touches the table, so it doesn't even need to exist.
        dbManager.registerMetadata(FileMetadata("EMPTYMETA", "EMPTYMETA_NONEXISTENT", emptyList(), emptyList()), true)
        val dbFile = dbManager.openFile("EMPTYMETA")
        val ex = assertFailsWith<IllegalArgumentException> { dbFile.chain(listOf("1")) }
        assertTrue(
            ex.message!!.contains("no primary key", ignoreCase = true),
            "Expected message to mention the missing primary key/fields, was: ${ex.message}",
        )
        dbManager.closeFile("EMPTYMETA")
    }

    @Test
    fun keyedFileTooManyKeysStillThrows() {
        // Regression coverage for checkKeys()'s pre-existing (previously untested) keyed-file
        // branch, to make sure relaxing the guard for RRN mode didn't loosen it for keyed files.
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val ex = assertFailsWith<IllegalArgumentException> {
            dbFile.chain(listOf("IT", "LOM", "BS", "ERBUSCO", "EXTRA"))
        }
        assertTrue(
            ex.message!!.contains("less than number of positioning/read keys"),
            "Expected message to mention the key-count mismatch, was: ${ex.message}"
        )
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }
}
