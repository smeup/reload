package com.smeup.dbnative.sql

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PostgreSQLDialectTest {

    private val dialect = PostgreSQLDialect()
    private val fileKeys = listOf("K1", "K2")
    private val posKeys = listOf("A", "B")
    private val identity: (List<String>) -> List<String> = { it }

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
