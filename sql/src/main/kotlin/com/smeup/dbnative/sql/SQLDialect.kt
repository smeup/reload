package com.smeup.dbnative.sql

import java.sql.Connection

interface SQLDialect {

    /**
     * Returns WHERE clause fragments (with their parameter values) for positioning-based reads.
     * Multiple fragments will be assembled into a UNION query by Native2SQL.
     * PostgreSQL returns one fragment; Default returns one per key level.
     */
    fun buildPositioningConditions(
        fileKeys: List<String>,
        positioningKeys: List<String>,
        method: PositioningMethod,
        forward: Boolean,
        buildReplacements: (List<String>) -> List<String>
    ): List<Pair<String, List<String>>>

    /**
     * Returns the JDBC fetch size hint for this dialect, or null to use the driver default.
     * Controls how many rows the driver retrieves per network round-trip.
     */
    fun fetchSize(): Int? = null

    /**
     * Called once, right after a new physical [Connection] is obtained (opened or borrowed
     * from a pool), before any query runs. Allows dialects to apply connection-scoped setup
     * for the whole lifetime of this connection (e.g. session timeouts, autoCommit mode).
     */
    fun onConnectionOpened(connection: Connection) {}

    /**
     * Called once, right before a [Connection] is closed or returned to a pool.
     * Must tolerate the connection already being dead/unusable.
     *
     * @param commit `true` to commit pending work, `false` to roll it back.
     */
    fun onConnectionClosing(connection: Connection, commit: Boolean) {}

    companion object {
        fun forUrl(url: String): SQLDialect = when {
            url.startsWith("jdbc:postgresql", ignoreCase = true) -> PostgreSQLDialect()
            else -> DefaultSQLDialect()
        }
    }
}

private fun comparisonFor(method: PositioningMethod, forward: Boolean): Pair<Comparison, Comparison> =
    when {
        forward  && method == PositioningMethod.SETLL -> Pair(Comparison.GE, Comparison.GT)
        forward  && method == PositioningMethod.SETGT -> Pair(Comparison.GT, Comparison.GT)
        !forward && method == PositioningMethod.SETLL -> Pair(Comparison.LT, Comparison.LT)
        else                                          -> Pair(Comparison.LE, Comparison.LT)
    }

class DefaultSQLDialect : SQLDialect {

    override fun buildPositioningConditions(
        fileKeys: List<String>,
        positioningKeys: List<String>,
        method: PositioningMethod,
        forward: Boolean,
        buildReplacements: (List<String>) -> List<String>
    ): List<Pair<String, List<String>>> {
        val (firstCmp, otherCmp) = comparisonFor(method, forward)
        return (positioningKeys.size downTo 1).map { i ->
            val where = (0 until i).joinToString(" AND ") { idx ->
                val cmp = when {
                    idx < i - 1               -> Comparison.EQ.symbol
                    i == positioningKeys.size -> firstCmp.symbol
                    else                      -> otherCmp.symbol
                }
                "\"${fileKeys[idx]}\" $cmp ?"
            }
            Pair(where, buildReplacements(positioningKeys.subList(0, i)))
        }
    }
}

class PostgreSQLDialect(
    // Safety net: a query's ResultSet may legitimately stay open (and its transaction with
    // it) across many separate native read calls, since callers aren't required to close it
    // promptly. Bounding how long the resulting transaction can sit idle prevents an abandoned
    // ResultSet from holding locks that block other connections (e.g. DDL) indefinitely.
    private val idleInTransactionTimeoutMs: Long = 30_000
) : SQLDialect {

    override fun fetchSize(): Int? = 100

    private var savedAutoCommit: Boolean? = null

    override fun onConnectionOpened(connection: Connection) {
        connection.createStatement().use {
            it.execute("SET idle_in_transaction_session_timeout = $idleInTransactionTimeoutMs")
        }
        savedAutoCommit = connection.autoCommit
        connection.autoCommit = false
    }

    override fun onConnectionClosing(connection: Connection, commit: Boolean) {
        savedAutoCommit?.let { saved ->
            try {
                if (!connection.autoCommit) {
                    if (commit) connection.commit() else connection.rollback()
                }
                connection.autoCommit = saved
            } catch (t: Throwable) {
                // Connection may already be dead, e.g. terminated server-side by
                // idle_in_transaction_session_timeout after being abandoned by the caller.
            }
            savedAutoCommit = null
        }
    }

    override fun buildPositioningConditions(
        fileKeys: List<String>,
        positioningKeys: List<String>,
        method: PositioningMethod,
        forward: Boolean,
        buildReplacements: (List<String>) -> List<String>
    ): List<Pair<String, List<String>>> {
        val lhs = positioningKeys.indices.joinToString(", ", "(", ")") { "\"${fileKeys[it]}\"" }
        val rhs = positioningKeys.joinToString(", ", "(", ")") { "?" }
        // Always use >= / <= so PostgreSQL can leverage the B-tree index on the range scan.
        // When the semantic requires strict exclusion (SETGT forward, SETLL backward), the exact
        // boundary row is filtered out with NOT (col = ? AND ...) rather than switching to > / <,
        // which can block index use on multi-column row-value comparisons.
        val cmp = if (forward) Comparison.GE else Comparison.LE
        val replacements = buildReplacements(positioningKeys)
        val needsExclusion = (forward && method == PositioningMethod.SETGT) ||
                             (!forward && method == PositioningMethod.SETLL)
        val (where, params) = if (needsExclusion) {
            val notEq = positioningKeys.indices.joinToString(" AND ") { "\"${fileKeys[it]}\" = ?" }
            "$lhs ${cmp.symbol} $rhs AND NOT ($notEq)" to replacements + replacements
        } else {
            "$lhs ${cmp.symbol} $rhs" to replacements
        }
        return listOf(Pair(where, params))
    }
}
