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
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.SQLException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Regression tests for the findings of the code review of the session tables (`__SID`).
 *
 * The tests of the first group reproduce real defects and FAIL until they are fixed; the second group adds coverage
 * of paths that work (READE/READP and page resume with other sessions interleaved).
 */
class SQLSessionTableReviewTest {

    companion object {
        private const val APP = "session-table-review-test"

        private val config = ConnectionConfig(
            fileName = "*",
            url = "jdbc:hsqldb:mem:SESSION_TABLE_REVIEW_TEST",
            user = "sa",
            password = "root"
        )
        private lateinit var manager: SQLDBMManager

        @BeforeClass
        @JvmStatic
        fun setUp() {
            manager = SQLDBMManager(config)
            val cfg = mapOf(APP to DBNativeAccessConfig(listOf(config)))
            ConnectionProvider.configure(cfg, cfg.mapValues { { c: ConnectionConfig -> SQLDBMManager(c) } })
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            manager.close()
        }

        private fun exec(vararg sql: String) = manager.connection.createStatement().use { st -> sql.forEach { st.execute(it) } }

        private fun sessionTable(name: String, uniqueKey: Boolean = true) = exec(
            "CREATE TABLE \"$name\" (\"__RNN\" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY, " +
                "\"__SID\" VARCHAR(64) NOT NULL, \"__TSTAMP\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, " +
                "\"K\" VARCHAR(10), \"V\" VARCHAR(10)" + (if (uniqueKey) ", UNIQUE(\"__SID\", \"K\"))" else ")")
        )

        private fun ordinaryTable(name: String) = exec(
            "CREATE TABLE \"$name\" (\"__RNN\" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY, " +
                "\"K\" VARCHAR(10), \"V\" VARCHAR(10))"
        )

        private fun metadata(table: String) =
            FileMetadata(table, table, listOf(Field("K", ""), Field("V", "")), listOf("K"))

        private fun record(k: String, v: String) = Record(RecordField("K", k), RecordField("V", v))

        private fun <T> inSession(sessionId: String?, block: () -> T): T {
            var result: T? = null
            ConnectionProvider.withScope(APP, ReloadContext(sessionId)) { result = block() }
            @Suppress("UNCHECKED_CAST")
            return result as T
        }

        private fun DBFile.readAll(): List<String> {
            val values = mutableListOf<String>()
            while (true) {
                val r = read()
                if (r.record.isEmpty()) break
                values.add(r.record["K"]!!.trim() + "=" + r.record["V"]!!.trim())
            }
            return values
        }
    }

    // ---- 1. the probe takes `_` of the table name as a wildcard ----------------------------------------------

    @Test
    fun anOrdinaryTableIsNotMistakenForASessionTableWhoseNameDiffersByAWildcardCharacter() {
        ordinaryTable("AB_C")
        sessionTable("ABXC")

        // `_` is a wildcard in a JDBC pattern: "AB_C" also matches "ABXC", which has __SID
        assertFalse(manager.connection.hasSessionColumnCached("AB_C", "__SID"))
        assertTrue(manager.connection.hasSessionColumnCached("ABXC", "__SID"))
    }

    @Test
    fun anOrdinaryTableWorksInASessionNextToASessionTableWithASimilarName() {
        ordinaryTable("FB_C")
        sessionTable("FBXC")

        // the ordinary table is written and read inside a session: it must not be given a __SID column
        inSession("S-WILDCARD") {
            val file = manager.openFile("FB_C", metadata("FB_C"))
            file.write(record("1", "one"))
            assertEquals("one", manager.openFile("FB_C", metadata("FB_C")).chain("1").record["V"]?.trim())
        }
    }

    @Test
    fun aTableThatDoesNotExistYetIsNotCachedAsOrdinaryBecauseOfASimilarlyNamedOne() {
        ordinaryTable("WKXA")

        // WK_A does not exist: "no __SID" must not be remembered (WKXA matches the pattern "WK_A")
        assertFalse(manager.connection.hasSessionColumnCached("WK_A", "__SID"))
        sessionTable("WK_A")

        assertTrue(
            manager.connection.hasSessionColumnCached("WK_A", "__SID"),
            "WK_A was created afterwards as a session table, it must be recognised"
        )
    }

    // ---- 2. a failing probe with no session bound must not leave a session table unfiltered ------------------

    @Test
    fun aFailingProbeWithNoSessionBoundFailsClosed() {
        sessionTable("PRB_T")
        exec("INSERT INTO \"PRB_T\" (\"__SID\", \"K\", \"V\") VALUES ('OWNER', '1', 'secret')")

        val real = manager.connection
        var failing = false
        val flaky = Proxy.newProxyInstance(
            Connection::class.java.classLoader, arrayOf(Connection::class.java)
        ) { _, method, args ->
            if (failing && method.name == "getMetaData") throw SQLException("metadata is temporarily unavailable")
            try {
                method.invoke(real, *(args ?: emptyArray()))
            } catch (e: InvocationTargetException) {
                throw e.targetException
            }
        } as Connection
        val file = SQLDBFile("PRB_T", metadata("PRB_T"), flaky)

        failing = true
        // no session is bound here: carrying on would read the rows of every session (the row of OWNER)
        assertFailsWith<IllegalStateException> { file.chain("1") }
    }

    // ---- 3. a blank session id is not a session ---------------------------------------------------------------

    @Test
    fun aBlankSessionIdIsNotASession() {
        sessionTable("BLK_T")
        exec("INSERT INTO \"BLK_T\" (\"__SID\", \"K\", \"V\") VALUES ('', '1', 'blank')")

        inSession("") { assertNull(ConnectionProvider.currentSessionIdOrNull()) }
        inSession("   ") { assertNull(ConnectionProvider.currentSessionIdOrNull()) }
        // so a session table fails closed instead of sharing the rows stamped with ''
        assertFailsWith<IllegalStateException> {
            inSession("") { manager.openFile("BLK_T", metadata("BLK_T")).chain("1") }
        }
    }

    // ---- coverage of paths that work: READE / READP / page resume with other sessions interleaved ------------

    @Test
    fun readEqualDoesNotReturnTheSameKeyOfAnotherSession() {
        sessionTable("RDE_T")
        inSession("S-RDE-A") { manager.openFile("RDE_T", metadata("RDE_T")).write(record("7", "a")) }
        inSession("S-RDE-B") { manager.openFile("RDE_T", metadata("RDE_T")).write(record("7", "b")) }

        inSession("S-RDE-A") {
            val file = manager.openFile("RDE_T", metadata("RDE_T"))
            file.setll("7")
            assertEquals("a", file.readEqual("7").record["V"]?.trim())
            assertTrue(file.readEqual("7").record.isEmpty(), "the row with the same key of session B is not ours")
        }
    }

    @Test
    fun pagedReadsForwardAndBackwardSeeOnlyTheRowsOfTheSessionWithOthersInterleaved() {
        sessionTable("PAG_T")
        val rows = 120
        // the rows of the two sessions are interleaved, so a page of 50 would mix them if it were not filtered
        for (i in 1..rows) {
            for (sid in listOf("S-PAG-A", "S-PAG-B")) {
                inSession(sid) { manager.openFile("PAG_T", metadata("PAG_T")).write(record("%04d".format(i), sid.takeLast(1))) }
            }
        }

        inSession("S-PAG-A") {
            val forward = SQLDBFile("PAG_T", metadata("PAG_T"), manager.connection, null, DefaultSQLDialect(50))
            forward.setll("0000")
            val all = forward.readAll()
            assertEquals(rows, all.size)
            assertTrue(all.all { it.endsWith("=A") }, "only the rows of A: $all")
            assertEquals((1..rows).map { "%04d=A".format(it) }, all)

            val backward = SQLDBFile("PAG_T", metadata("PAG_T"), manager.connection, null, DefaultSQLDialect(50))
            backward.setll("9999")
            val reversed = mutableListOf<String>()
            while (true) {
                val r = backward.readPrevious()
                if (r.record.isEmpty()) break
                reversed.add(r.record["K"]!!.trim() + "=" + r.record["V"]!!.trim())
            }
            assertEquals((rows downTo 1).map { "%04d=A".format(it) }, reversed)
        }
    }
}
