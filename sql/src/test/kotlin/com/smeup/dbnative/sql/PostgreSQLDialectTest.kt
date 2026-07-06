package com.smeup.dbnative.sql

import org.junit.After
import org.junit.Test
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Fake [Connection] backed by a real HSQLDB connection (for members we don't care about),
 * with [createStatement]/`autoCommit`/[commit]/[rollback] intercepted and recorded so
 * [PostgreSQLDialect]'s connection-lifecycle hooks can be tested without a real PostgreSQL
 * instance. `execute` on statements created here is recorded but never actually run against
 * HSQLDB, since [PostgreSQLDialect] issues PostgreSQL-only syntax.
 */
private class TrackingConnection(private val real: Connection) : Connection by real {
    val executedSql = mutableListOf<String>()
    var commitCount = 0
    var rollbackCount = 0
    var throwOnCommit = false
    var throwOnRollback = false
    private var autoCommitState = real.autoCommit

    override fun createStatement(): Statement {
        val realStatement = real.createStatement()
        return object : Statement by realStatement {
            override fun execute(sql: String): Boolean {
                executedSql.add(sql)
                return true
            }
        }
    }

    override fun getAutoCommit(): Boolean = autoCommitState
    override fun setAutoCommit(autoCommit: Boolean) { autoCommitState = autoCommit }

    override fun commit() {
        commitCount++
        if (throwOnCommit) throw RuntimeException("connection is dead")
    }

    override fun rollback() {
        rollbackCount++
        if (throwOnRollback) throw RuntimeException("connection is dead")
    }
}

class PostgreSQLDialectTest {

    private val dialect = PostgreSQLDialect()
    private val fileKeys = listOf("K1", "K2")
    private val posKeys = listOf("A", "B")
    private val identity: (List<String>) -> List<String> = { it }

    private fun newTrackingConnection() =
        TrackingConnection(DriverManager.getConnection("jdbc:hsqldb:mem:PG_DIALECT_TEST", "sa", "root"))

    @After
    fun tearDown() {
        DriverManager.getConnection("jdbc:hsqldb:mem:PG_DIALECT_TEST;shutdown=true", "sa", "root").runCatching { close() }
    }

    @Test
    fun `onConnectionOpened sets idle timeout and disables autoCommit`() {
        val connection = newTrackingConnection()
        connection.autoCommit = true

        dialect.onConnectionOpened(connection)

        assertEquals(1, connection.executedSql.size)
        assertTrue(connection.executedSql[0].contains("idle_in_transaction_session_timeout"))
        assertFalse(connection.autoCommit)
    }

    @Test
    fun `onConnectionClosing commits and restores autoCommit when commit is true`() {
        val connection = newTrackingConnection()
        connection.autoCommit = true
        dialect.onConnectionOpened(connection)

        dialect.onConnectionClosing(connection, commit = true)

        assertEquals(1, connection.commitCount)
        assertEquals(0, connection.rollbackCount)
        assertTrue(connection.autoCommit)
    }

    @Test
    fun `onConnectionClosing rolls back and restores autoCommit when commit is false`() {
        val connection = newTrackingConnection()
        connection.autoCommit = true
        dialect.onConnectionOpened(connection)

        dialect.onConnectionClosing(connection, commit = false)

        assertEquals(0, connection.commitCount)
        assertEquals(1, connection.rollbackCount)
        assertTrue(connection.autoCommit)
    }

    @Test
    fun `onConnectionClosing swallows exception from a dead connection`() {
        val connection = newTrackingConnection()
        connection.autoCommit = true
        dialect.onConnectionOpened(connection)
        connection.throwOnCommit = true

        dialect.onConnectionClosing(connection, commit = true)
        // no exception propagated
    }

    @Test
    fun `SETLL forward uses GE without exclusion`() {
        val result = dialect.buildPositioningConditions(fileKeys, posKeys, PositioningMethod.SETLL, true, identity)
        assertEquals(1, result.size)
        val (where, params) = result[0]
        assertTrue(where.contains(">="), "Expected >= in: $where")
        assertFalse(where.contains("NOT"), "Expected no NOT in: $where")
        assertEquals(posKeys, params)
    }

    @Test
    fun `SETGT forward uses GE with NOT exclusion`() {
        val result = dialect.buildPositioningConditions(fileKeys, posKeys, PositioningMethod.SETGT, true, identity)
        assertEquals(1, result.size)
        val (where, params) = result[0]
        assertTrue(where.contains(">="), "Expected >= in: $where")
        assertTrue(where.contains("NOT"), "Expected NOT in: $where")
        // params duplicated: once for >= and once for NOT equality check
        assertEquals(posKeys + posKeys, params)
    }

    @Test
    fun `SETLL backward uses LE with NOT exclusion`() {
        val result = dialect.buildPositioningConditions(fileKeys, posKeys, PositioningMethod.SETLL, false, identity)
        assertEquals(1, result.size)
        val (where, params) = result[0]
        assertTrue(where.contains("<="), "Expected <= in: $where")
        assertTrue(where.contains("NOT"), "Expected NOT in: $where")
        assertEquals(posKeys + posKeys, params)
    }

    @Test
    fun `SETGT backward uses LE without exclusion`() {
        val result = dialect.buildPositioningConditions(fileKeys, posKeys, PositioningMethod.SETGT, false, identity)
        assertEquals(1, result.size)
        val (where, params) = result[0]
        assertTrue(where.contains("<="), "Expected <= in: $where")
        assertFalse(where.contains("NOT"), "Expected no NOT in: $where")
        assertEquals(posKeys, params)
    }

    @Test
    fun `generated SQL shape for SETGT forward`() {
        val result = dialect.buildPositioningConditions(fileKeys, posKeys, PositioningMethod.SETGT, true, identity)
        val (where, _) = result[0]
        assertEquals("(\"K1\", \"K2\") >= (?, ?) AND NOT (\"K1\" = ? AND \"K2\" = ?)", where)
    }

    @Test
    fun `generated SQL shape for SETLL backward`() {
        val result = dialect.buildPositioningConditions(fileKeys, posKeys, PositioningMethod.SETLL, false, identity)
        val (where, _) = result[0]
        assertEquals("(\"K1\", \"K2\") <= (?, ?) AND NOT (\"K1\" = ? AND \"K2\" = ?)", where)
    }
}
