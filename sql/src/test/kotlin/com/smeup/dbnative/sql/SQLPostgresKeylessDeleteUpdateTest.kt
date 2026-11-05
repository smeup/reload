/*
 *  Copyright 2023 The Reload project Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package com.smeup.dbnative.sql

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.sql.utils.TestSQLDBType
import com.smeup.dbnative.sql.utils.createFile
import com.smeup.dbnative.sql.utils.dbManagerForTest
import com.smeup.dbnative.sql.utils.destroyDatabase
import com.smeup.dbnative.utils.TypedMetadata
import com.smeup.dbnative.utils.fieldByType
import org.junit.AfterClass
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Positioned DELETE/UPDATE on a PostgreSQL file with no declared keys.
 *
 * pgjdbc allows `ResultSet.deleteRow()`/`updateRow()` only when a primary or unique key of the table
 * is selected under its own name. A keyless file has only `__RNN` as primary key, which reload
 * projects aliased (`AS "RRN__"`), so the driver rejects the positioned operation with
 * "No eligible primary or unique key found for table" (SQLSTATE 24000).
 * [PostgreSQLDialect.isReadOnlyCursorError] maps that to the searched DELETE/UPDATE by `__RNN`
 * fallback, which is what these tests exercise against a real PostgreSQL.
 *
 * Always runs against PostgreSQL (whatever the default test database is) and is skipped when it is
 * not reachable, so it needs the `docker/postgresql` container locally.
 */
class SQLPostgresKeylessDeleteUpdateTest {

    companion object {

        private const val TABLE = "KEYLESS_PG"

        private var dbManager: SQLDBMManager? = null

        private val fields = listOf(
            "A" fieldByType CharacterType(10),
            "B" fieldByType CharacterType(10)
        )

        @BeforeClass
        @JvmStatic
        fun setUp() {
            val manager = try {
                dbManagerForTest(TestSQLDBType.POSTGRES).also { it.connection.isValid(5) }
            } catch (e: Exception) {
                Assume.assumeNoException("PostgreSQL not reachable, skipping", e)
                return
            }
            dbManager = manager
            // __RNN is the only key: the file declares none (empty key list)
            createFile(TypedMetadata(TABLE, TABLE, fields, emptyList()), manager)
            manager.openFile(TABLE).let { file ->
                listOf("ONE" to "x", "TWO" to "y", "THREE" to "z").forEach { (a, b) ->
                    file.write(Record(RecordField("A", a), RecordField("B", b)))
                }
            }
            manager.closeFile(TABLE)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dbManager?.close()
            if (dbManager != null) destroyDatabase(TestSQLDBType.POSTGRES)
        }
    }

    private fun rowCount(): Int =
        dbManager!!.connection.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM \"$TABLE\"").use { rs -> rs.next(); rs.getInt(1) }
        }

    @Test
    fun deleteOnKeylessFileFallsBackToRrn() {
        val manager = dbManager!!
        val before = rowCount()
        val dbFile = manager.openFile(TABLE)

        val chained = dbFile.chain(listOf("2"))
        assertFalse(dbFile.eof())
        assertEquals("TWO", chained.record["A"]?.trim())

        // Used to fail with: No eligible primary or unique key found for table "KEYLESS_PG"
        dbFile.delete(chained.record)

        assertEquals(before - 1, rowCount(), "exactly the current row must be deleted")
        dbFile.chain(listOf("2"))
        assertTrue(dbFile.eof(), "RRN 2 must be gone")
        assertEquals("ONE", dbFile.chain(listOf("1")).record["A"]?.trim(), "other rows must be untouched")
        assertEquals("THREE", dbFile.chain(listOf("3")).record["A"]?.trim(), "other rows must be untouched")
        manager.closeFile(TABLE)
    }

    @Test
    fun updateOnKeylessFileFallsBackToRrn() {
        val manager = dbManager!!
        val dbFile = manager.openFile(TABLE)

        val chained = dbFile.chain(listOf("3"))
        assertEquals("THREE", chained.record["A"]?.trim())
        val record = chained.record
        record["B"] = "updated"

        // Used to fail with: No eligible primary or unique key found for table "KEYLESS_PG"
        dbFile.update(record)

        val after = dbFile.chain(listOf("3")).record
        assertEquals("updated", after["B"]?.trim())
        assertEquals("THREE", after["A"]?.trim())
        assertEquals("x", dbFile.chain(listOf("1")).record["B"]?.trim(), "other rows must be untouched")
        manager.closeFile(TABLE)
    }
}
