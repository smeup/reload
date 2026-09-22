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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Covers Relative Record Number (RRN) access (Native2SQLAdapter's `rrnMode`, see checkKeys()):
 * CHAIN by RRN against a file whose metadata declares no keys is resolved by
 * [SQLDialect.rrnSelectExpression] - the convention `__RNN` identity column on every database
 * except DB2 for i (see SQLDBTestUtils.createFile, which gives every test table one), so RRN
 * follows insertion order and behaves the same on every test database. The same expression is
 * projected as the output RRN for keyed files too.
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
    fun chainByRRN() {
        // EMPLOYEE_RRN: same physical table as EMPLOYEE, but declared unkeyed - forces RRN mode,
        // so CHAIN positions by the table's __RNN (Employee.csv rows are loaded in EMPNO order).
        dbManager.registerMetadata(FileMetadata("EMPLOYEE_RRN", EMPLOYEE_TABLE_NAME, employeeFields, emptyList()), true)
        val dbFile = dbManager.openFile("EMPLOYEE_RRN")
        val result = dbFile.chain(listOf("2"))
        assertEquals("000020", result.record["EMPNO"]?.trim())
        assertEquals("THOMPSON", result.record["LASTNAME"]?.trim())
        // Output-RRN direction: the row's RRN comes back on Result.rrn, matching the RRN it was
        // chained by, and is not leaked into the RPG-visible record fields.
        assertEquals(2L, result.rrn)
        assertTrue("EMPNO" in result.record.keys)
        assertFalse("RRN__" in result.record.keys)
        dbManager.closeFile("EMPLOYEE_RRN")
    }

    @Test
    fun readEqualAfterSetllPopulatesRrn() {
        // SETLL+READE (a positioning-based read, going through buildDialectPositioningSQL/
        // getSQLOrderByClause - a different code path than CHAIN's getSQL) must populate
        // Result.rrn exactly like CHAIN does: outerColumns() is shared by both.
        dbManager.registerMetadata(FileMetadata("EMPLOYEE_RRN2", EMPLOYEE_TABLE_NAME, employeeFields, emptyList()), true)
        val dbFile = dbManager.openFile("EMPLOYEE_RRN2")
        dbFile.setll(listOf("2"))
        val result = dbFile.readEqual(listOf("2"))
        assertEquals("000020", result.record["EMPNO"]?.trim())
        assertEquals(2L, result.rrn)
        dbManager.closeFile("EMPLOYEE_RRN2")
    }

    @Test
    fun chainByRRNFollowsInsertionOrder() {
        // NOPKTABLE has no keys at all. RRN is the row's __RNN identity value, so RRN=1 is
        // whichever row was inserted first ("C"), regardless of CODE/DESCR ordering - rows are
        // inserted in a deliberately non-alphabetical order to prove it.
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
                "INSERT INTO \"NOPKTABLE\" (\"CODE\", \"DESCR\") VALUES ('C', 'third')",
                "INSERT INTO \"NOPKTABLE\" (\"CODE\", \"DESCR\") VALUES ('A', 'first')",
                "INSERT INTO \"NOPKTABLE\" (\"CODE\", \"DESCR\") VALUES ('B', 'second')",
            ),
        )
        val dbFile = dbManager.openFile("NOPKTABLE")
        val result = dbFile.chain(listOf("1"))
        assertEquals("C", result.record["CODE"]?.trim())
        assertEquals("third", result.record["DESCR"]?.trim())
        dbManager.closeFile("NOPKTABLE")
    }

    @Test
    fun keyedFileReadHasOutputRrn() {
        // A keyed file is still positioned by its real keys, but the RRN expression is projected
        // as an output column too (no FROM restructuring, so the ResultSet stays updatable), so
        // Result.rrn is a real, non-null value on every dialect.
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val result = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertEquals("ERBUSCO", result.record["CITTA"]?.trim())
        assertTrue(result.rrn != null && result.rrn!! > 0, "Expected a real RRN, was: ${result.rrn}")
        assertFalse("RRN__" in result.record.keys)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
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
