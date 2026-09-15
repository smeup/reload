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
 * and falling back to an ordering view's declared ORDER BY, per [Connection.primaryKeys] /
 * [Connection.orderingFields] in JDBCUtils.kt.
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
            createAndPopulateEmployeeView(dbManager)
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
    fun chainByRRNUsingViewOrderingFallback() {
        // EMPLOYEE_VIEW_RRN: same view as EMPLOYEE_VIEW, but declared unkeyed - the view has no
        // primary key, so connection.primaryKeys() returns empty and RRN falls back to
        // connection.orderingFields(), which parses the view's declared ORDER BY (WORKDEPT, EMPNO).
        dbManager.registerMetadata(FileMetadata("EMPLOYEE_VIEW_RRN", EMPLOYEE_VIEW_NAME, employeeFields, emptyList()), true)
        val dbFile = dbManager.openFile("EMPLOYEE_VIEW_RRN")
        val result = dbFile.chain(listOf("2"))
        // Second row ordered by (WORKDEPT, EMPNO): WORKDEPT="A00" rows ordered by EMPNO are
        // 000010, 000110, 000120, 200010, 200120 - RRN 2 is EMPNO 000110 (LUCCHESSI), distinct
        // from the PK-only ordering test above (which lands on EMPNO 000020 at RRN 2).
        assertEquals("000110", result.record["EMPNO"]?.trim())
        assertEquals("LUCCHESSI", result.record["LASTNAME"]?.trim())
        dbManager.closeFile("EMPLOYEE_VIEW_RRN")
    }

    @Test
    fun chainByRRNWithNoResolvableOrderingThrows() {
        // A fresh table with no primary key and not a view: neither primaryKeys() nor
        // orderingFields() can resolve a deterministic order, so RRN access must fail clearly
        // instead of silently returning a nondeterministically-ordered row.
        val typedFields = listOf(
            "EMPNO" fieldByType CharacterType(6),
            "FIRSTNME" fieldByType CharacterType(12)
        )
        createFile(TypedMetadata("NOORDERTBL", "NOORDERTBL", typedFields, emptyList()), dbManager)
        val dbFile = dbManager.openFile("NOORDERTBL")
        val ex = assertFailsWith<IllegalArgumentException> { dbFile.chain(listOf("1")) }
        assertTrue(
            ex.message!!.contains("no primary key", ignoreCase = true),
            "Expected message to mention the missing primary key/ordering, was: ${ex.message}"
        )
        dbManager.closeFile("NOORDERTBL")
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
