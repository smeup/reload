/*
 * Copyright 2026 The Reload project Authors
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

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.ReloadContext
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Session tables: a table with the `__SID` column is restricted, on every operation, to the rows of the
 * session bound to the [ConnectionProvider] scope ([ReloadContext.sessionId]).
 */
class SQLSessionTableTest {

    companion object {
        private const val APP = "session-table-test"
        private const val KEYED = "SESS_KEYED"
        private const val UNKEYED = "SESS_UNKEYED"
        private const val ORDINARY = "SESS_ORDINARY"

        private val config = ConnectionConfig(
            fileName = "*",
            url = "jdbc:hsqldb:mem:SESSION_TABLE_TEST",
            user = "sa",
            password = "root"
        )
        private lateinit var manager: SQLDBMManager

        private val keyedMetadata = FileMetadata(KEYED, KEYED, listOf(Field("K", ""), Field("V", "")), listOf("K"))
        private val unkeyedMetadata = FileMetadata(UNKEYED, UNKEYED, listOf(Field("K", ""), Field("V", "")), emptyList())
        private val ordinaryMetadata = FileMetadata(ORDINARY, ORDINARY, listOf(Field("K", ""), Field("V", "")), listOf("K"))

        @BeforeClass
        @JvmStatic
        fun setUp() {
            manager = SQLDBMManager(config)
            val cfg = mapOf(APP to DBNativeAccessConfig(listOf(config)))
            ConnectionProvider.configure(cfg, cfg.mapValues { { c: ConnectionConfig -> SQLDBMManager(c) } })
            manager.connection.createStatement().use { st ->
                // same key in different sessions is allowed, so the key is unique only together with __SID
                st.execute(
                    "CREATE TABLE \"$KEYED\" (\"__RNN\" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY, " +
                        "\"__SID\" VARCHAR(64) NOT NULL, \"__TSTAMP\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, " +
                        "\"K\" VARCHAR(10), \"V\" VARCHAR(10), UNIQUE(\"__SID\", \"K\"))"
                )
                st.execute("CREATE INDEX \"${KEYED}_SID\" ON \"$KEYED\" (\"__SID\")")
                st.execute(
                    "CREATE TABLE \"$UNKEYED\" (\"__RNN\" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY, " +
                        "\"__SID\" VARCHAR(64) NOT NULL, \"__TSTAMP\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, " +
                        "\"K\" VARCHAR(10), \"V\" VARCHAR(10))"
                )
                st.execute(
                    "CREATE TABLE \"$ORDINARY\" (\"__RNN\" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY, " +
                        "\"K\" VARCHAR(10), \"V\" VARCHAR(10), UNIQUE(\"K\"))"
                )
            }
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            manager.close()
        }

        private fun record(k: String, v: String, vararg extra: RecordField) =
            Record(RecordField("K", k), RecordField("V", v), *extra)

        private fun <T> inSession(sessionId: String?, block: () -> T): T {
            var result: T? = null
            ConnectionProvider.withScope(APP, ReloadContext(sessionId)) { result = block() }
            @Suppress("UNCHECKED_CAST")
            return result as T
        }

        private fun DBFile.readAllValues(): List<String> {
            val values = mutableListOf<String>()
            while (true) {
                val r = read()
                if (r.record.isEmpty()) break
                values.add(r.record["K"]!!.trim() + "=" + r.record["V"]!!.trim())
            }
            return values.sorted()
        }

        private fun rowsOf(table: String, sessionId: String): Int =
            manager.connection.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM \"$table\" WHERE \"__SID\" = '$sessionId'").use { it.next(); it.getInt(1) }
            }
    }

    @Test
    fun readsAndChainsAreRestrictedToTheCurrentSession() {
        inSession("S-READ-A") {
            val f = manager.openFile(KEYED, keyedMetadata)
            f.write(record("1", "a1"))
            f.write(record("2", "a2"))
        }
        inSession("S-READ-B") {
            manager.openFile(KEYED, keyedMetadata).write(record("1", "b1"))
        }

        inSession("S-READ-A") {
            val f = manager.openFile(KEYED, keyedMetadata)
            assertEquals("a1", f.chain("1").record["V"]?.trim())
            assertEquals(listOf("1=a1", "2=a2"), manager.openFile(KEYED, keyedMetadata).readAllValues())
        }
        inSession("S-READ-B") {
            val f = manager.openFile(KEYED, keyedMetadata)
            assertEquals("b1", f.chain("1").record["V"]?.trim())
            assertTrue(manager.openFile(KEYED, keyedMetadata).chain("2").record.isEmpty(), "key 2 belongs to session A")
            assertEquals(listOf("1=b1"), manager.openFile(KEYED, keyedMetadata).readAllValues())
        }
    }

    @Test
    fun positioningReadsAreRestrictedToTheCurrentSession() {
        inSession("S-POS-A") {
            val f = manager.openFile(KEYED, keyedMetadata)
            f.write(record("1", "a1"))
            f.write(record("3", "a3"))
        }
        inSession("S-POS-B") {
            val f = manager.openFile(KEYED, keyedMetadata)
            f.write(record("2", "b2"))
            f.write(record("4", "b4"))
        }

        inSession("S-POS-A") {
            val f = manager.openFile(KEYED, keyedMetadata)
            f.setll("0")
            val values = mutableListOf<String>()
            while (true) {
                val r = f.read()
                if (r.record.isEmpty()) break
                values.add(r.record["K"]!!.trim())
            }
            // 2 and 4 are session B's rows: they are not seen, not even as the "next" ones
            assertEquals(listOf("1", "3"), values)
        }
    }

    @Test
    fun writeIgnoresTheSessionIdSentByTheCaller() {
        inSession("S-FORGE") {
            manager.openFile(KEYED, keyedMetadata).write(record("9", "x", RecordField("__SID", "SOMEONE-ELSE")))
        }
        assertEquals(1, rowsOf(KEYED, "S-FORGE"))
        assertEquals(0, rowsOf(KEYED, "SOMEONE-ELSE"))
    }

    @Test
    fun updateAndDeleteTouchOnlyTheRowsOfTheCurrentSession() {
        inSession("S-UPD-A") { manager.openFile(KEYED, keyedMetadata).write(record("1", "a1")) }
        inSession("S-UPD-B") { manager.openFile(KEYED, keyedMetadata).write(record("1", "b1")) }

        inSession("S-UPD-A") {
            val f = manager.openFile(KEYED, keyedMetadata)
            val chained = f.chain("1").record
            f.update(record("1", "a1-updated"))
            assertEquals("a1-updated", manager.openFile(KEYED, keyedMetadata).chain("1").record["V"]?.trim())
            assertEquals("a1", chained["V"]?.trim())
        }
        inSession("S-UPD-B") {
            assertEquals("b1", manager.openFile(KEYED, keyedMetadata).chain("1").record["V"]?.trim())
        }

        inSession("S-UPD-A") {
            val f = manager.openFile(KEYED, keyedMetadata)
            f.chain("1")
            f.delete(Record())
        }
        assertEquals(0, rowsOf(KEYED, "S-UPD-A"))
        assertEquals(1, rowsOf(KEYED, "S-UPD-B"))
    }

    @Test
    fun chainByRrnDoesNotSeeTheRowsOfAnotherSession() {
        inSession("S-RRN-A") { manager.openFile(UNKEYED, unkeyedMetadata).write(record("1", "a1")) }
        var rrnOfB: Long? = null
        inSession("S-RRN-B") {
            manager.openFile(UNKEYED, unkeyedMetadata).write(record("1", "b1"))
            val f = manager.openFile(UNKEYED, unkeyedMetadata)
            val r = f.read()
            assertEquals("b1", r.record["V"]?.trim())
            rrnOfB = r.rrn
        }
        assertTrue(rrnOfB != null, "the RRN of the row of session B is read back")

        inSession("S-RRN-B") {
            assertEquals("b1", manager.openFile(UNKEYED, unkeyedMetadata).chain(rrnOfB.toString()).record["V"]?.trim())
        }
        inSession("S-RRN-A") {
            assertTrue(
                manager.openFile(UNKEYED, unkeyedMetadata).chain(rrnOfB.toString()).record.isEmpty(),
                "the row with that RRN belongs to session B"
            )
        }
    }

    @Test
    fun usingASessionTableWithoutASessionFails() {
        // the check is done at the first operation (see SQLDBFile.sessionId), not when the file is opened
        assertFailsWith<IllegalStateException> {
            inSession(null) { manager.openFile(KEYED, keyedMetadata).chain("1") }
        }
        assertFailsWith<IllegalStateException> {
            manager.openFile(KEYED, keyedMetadata).chain("1") // no scope at all
        }
    }

    /**
     * Regression: an application may open a file (jariko does it when the program starts) before the table exists
     * and create it afterwards (a work file created on demand). The file must be filtered all the same, and the
     * "no __SID column" answer given while the table did not exist must not be remembered.
     */
    @Test
    fun sessionTableCreatedAfterTheFileWasOpenedIsStillFiltered() {
        val table = "SESS_LATE"
        val metadata = FileMetadata(table, table, listOf(Field("K", ""), Field("V", "")), listOf("K"))
        var lateFile: DBFile? = null
        inSession("S-LATE-A") {
            lateFile = manager.openFile(table, metadata) // the table does not exist yet
        }
        manager.connection.createStatement().use { st ->
            st.execute(
                "CREATE TABLE \"$table\" (\"__RNN\" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY, " +
                    "\"__SID\" VARCHAR(64) NOT NULL, \"__TSTAMP\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, " +
                    "\"K\" VARCHAR(10), \"V\" VARCHAR(10))"
            )
        }
        inSession("S-LATE-A") {
            lateFile!!.write(record("1", "a"))
            manager.openFile(table, metadata).write(record("1", "a2"))
        }
        inSession("S-LATE-B") {
            manager.openFile(table, metadata).write(record("1", "b"))
        }
        inSession("S-LATE-B") {
            // B sees its own row only, through a file opened after the creation...
            assertEquals(listOf("1=b"), manager.openFile(table, metadata).readAllValues())
        }
        // ...and a file opened before it (the first one, which resolves the session lazily) is filtered as well
        inSession("S-LATE-B") {
            assertEquals(listOf("1=b"), manager.openFile(table, metadata).readAllValues())
        }
        assertEquals(2, rowsOf(table, "S-LATE-A"))
        assertEquals(1, rowsOf(table, "S-LATE-B"))
    }

    @Test
    fun ordinaryTablesAreNotAffectedByTheSession() {
        inSession("S-ORD-A") { manager.openFile(ORDINARY, ordinaryMetadata).write(record("1", "shared")) }
        inSession("S-ORD-B") {
            assertEquals("shared", manager.openFile(ORDINARY, ordinaryMetadata).chain("1").record["V"]?.trim())
        }
        // and outside of any scope
        assertEquals("shared", manager.openFile(ORDINARY, ordinaryMetadata).chain("1").record["V"]?.trim())
    }
}
