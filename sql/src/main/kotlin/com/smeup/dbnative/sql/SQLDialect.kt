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
     * Max rows a single positioning-based SELECT returns (via `FETCH FIRST n ROWS ONLY`),
     * or null for no cap. Standard SQL, supported by PostgreSQL, DB2/AS400, and HSQLDB: without
     * it, the query is planned as an unbounded range scan, since the only signal the optimizer
     * has that not all matching rows will be fetched is a generic assumption (e.g. PostgreSQL's
     * cursor_tuple_fraction, ~10% of the match by default). Native2SQL transparently re-queries
     * for the next page once a page is fully consumed, so this is invisible to DBFile callers.
     */
    fun pageSize(): Int? = 100

    /**
     * A derived-table expression that numbers every row of [fromTable] by [orderByColumns]
     * (ascending) as a synthetic column aliased [rrnColumn], used to derive Relative Record
     * Number access on an unkeyed file via `ROW_NUMBER()`. [columns] are the real column names to
     * project through unchanged (never `*`, to avoid ambiguity/duplicate-column errors on some
     * engines when combined with a computed column). Returns a parenthesized subquery with no
     * outer alias - the caller adds one.
     *
     * Default implementation pre-sorts in a nested derived table and numbers rows with an
     * order-less `ROW_NUMBER() OVER ()` on top of it, rather than the more direct
     * `ROW_NUMBER() OVER (ORDER BY ...)`: not every engine's window-function grammar accepts an
     * ORDER BY inside OVER() (verified: HSQLDB, used by this project's own test suite, rejects it
     * outright with a syntax error), while a simple, unshuffled wrapping SELECT reliably preserves
     * its child's materialized row order in every engine this project targets. Override this for
     * an engine confirmed to support the native ORDER BY-in-OVER() form (see [PostgreSQLDialect],
     * [DB2400Dialect]) for a more directly-expressed, equally correct query.
     */
    fun buildRowNumberedSubquery(columns: String, fromTable: String, orderByColumns: List<String>, rrnColumn: String): String {
        val orderBy = orderByColumns.joinToString(", ") { "\"$it\"" }
        return "(SELECT $columns, ROW_NUMBER() OVER () AS \"$rrnColumn\" " +
            "FROM (SELECT $columns FROM $fromTable ORDER BY $orderBy) \"${rrnColumn}_SORT\")"
    }

    /**
     * SQL expression that yields this row's RRN as an ordinary projected column, for engines where
     * RRN is available as a per-row value without restructuring the query (DB2 for i's native
     * `RRN()` function; PostgreSQL's convention `__RNN` identity column). [tableAlias] is the alias
     * (or, absent one, the quoted table name itself) the query already uses for the base table.
     * Returns null when this dialect has no such direct expression and must instead synthesize an
     * RRN via [buildRowNumberedSubquery] (the [DefaultSQLDialect] case).
     */
    fun rrnSelectExpression(tableAlias: String): String? = null

    /**
     * The SQL placeholder text to bind an RRN value against, wherever [rrnSelectExpression] is
     * compared to a bound parameter (query-by-RRN positioning/equality). Default: a plain `?`.
     * Override when the direct expression is a real typed column and the driver needs an explicit
     * cast to resolve the comparison - PostgreSQL's JDBC driver binds every reload parameter as
     * VARCHAR (reload's whole binding pipeline is string-based, see [SQLDBFile]'s `bind()`), and
     * PostgreSQL refuses to compare a `bigint` column against an un-cast VARCHAR parameter
     * ("operator does not exist: bigint = character varying" - confirmed against a real table's
     * `__RNN` column).
     */
    fun rrnParameterPlaceholder(): String = "?"

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
        fun forUrl(url: String, pageSize: Int? = null): SQLDialect = when {
            url.startsWith("jdbc:postgresql", ignoreCase = true) -> PostgreSQLDialect(pageSize = pageSize)
            url.startsWith("jdbc:as400", ignoreCase = true) -> DB2400Dialect(pageSize = pageSize)
            else -> DefaultSQLDialect(pageSize = pageSize)
        }
    }
}

/**
 * Resolves the raw `reload.dialect.pageSize` value (as passed through by [SQLDialect.forUrl])
 * against a dialect's own default: `null` means the property wasn't set, so [default] applies;
 * zero or negative is an explicit opt-out, meaning no cap regardless of [default].
 */
private fun resolvePageSize(pageSize: Int?, default: Int?): Int? = when {
    pageSize == null -> default
    pageSize <= 0 -> null
    else -> pageSize
}

/** Internal (not private) so [Native2SQL] can reuse it to build a `WHERE` fragment for RRN-mode
 *  positioning directly off [SQLDialect.rrnSelectExpression] (DB2, PostgreSQL), bypassing
 *  [SQLDialect.buildPositioningConditions]'s general multi-key machinery - RRN-mode positioning
 *  is always single-key, so the per-key-level UNION strategy that exists for real key tuples is
 *  unneeded there. */
internal fun comparisonFor(method: PositioningMethod, forward: Boolean): Pair<Comparison, Comparison> =
    when {
        forward  && method == PositioningMethod.SETLL -> Pair(Comparison.GE, Comparison.GT)
        forward  && method == PositioningMethod.SETGT -> Pair(Comparison.GT, Comparison.GT)
        !forward && method == PositioningMethod.SETLL -> Pair(Comparison.LT, Comparison.LT)
        else                                          -> Pair(Comparison.LE, Comparison.LT)
    }

/**
 * Per-key-level UNION strategy: one fragment per key-prefix length, combined by Native2SQL
 * with UNION. Portable to any engine, since it doesn't rely on row-value constructor support.
 */
private fun unionPositioningConditions(
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

class DefaultSQLDialect(pageSize: Int? = null) : SQLDialect {

    private val pageSize: Int? = resolvePageSize(pageSize, default = 100)

    override fun pageSize(): Int? = pageSize

    override fun buildPositioningConditions(
        fileKeys: List<String>,
        positioningKeys: List<String>,
        method: PositioningMethod,
        forward: Boolean,
        buildReplacements: (List<String>) -> List<String>
    ): List<Pair<String, List<String>>> =
        unionPositioningConditions(fileKeys, positioningKeys, method, forward, buildReplacements)
}

/**
 * DB2 for i (AS400), reached via the IBM Toolbox JDBC driver (`jdbc:as400://...`,
 * com.ibm.as400.access.AS400JDBCDriver). Uses the same portable per-key-level UNION strategy
 * as [DefaultSQLDialect] rather than PostgreSQL's row-value tuple comparison, since row-value
 * constructor support for inequality comparisons isn't confirmed across DB2-for-i versions.
 */
class DB2400Dialect(pageSize: Int? = null) : SQLDialect {

    // Unlike the other dialects, absent config leaves this uncapped by default: row-value/FETCH
    // FIRST behavior across DB2-for-i versions isn't validated yet, so opt-in via
    // reload.dialect.pageSize until that's confirmed on real AS400 hardware.
    private val pageSize: Int? = resolvePageSize(pageSize, default = null)

    override fun pageSize(): Int? = pageSize

    override fun buildPositioningConditions(
        fileKeys: List<String>,
        positioningKeys: List<String>,
        method: PositioningMethod,
        forward: Boolean,
        buildReplacements: (List<String>) -> List<String>
    ): List<Pair<String, List<String>>> =
        unionPositioningConditions(fileKeys, positioningKeys, method, forward, buildReplacements)

    // No buildRowNumberedSubquery override: rrnSelectExpression below gives Native2SQL a direct
    // RRN expression, so it never needs to fall back to the ROW_NUMBER()-wrapped-FROM technique
    // (that fallback, and its "native ORDER BY inside OVER()" override, is DefaultSQLDialect-only
    // territory now - see Native2SQLAdapter.tableExpr/directRrnExpr).

    // DB2 for i's native RRN() scalar function: the real physical relative record number,
    // maintained by the engine - exactly what IBM i RPG's %RRN()/INFDS already means. No
    // computation, no ordering-consistency problem, unlike the ROW_NUMBER() fallback.
    override fun rrnSelectExpression(tableAlias: String): String = "RRN($tableAlias)"
}

class PostgreSQLDialect(pageSize: Int? = null) : SQLDialect {

    private val pageSize: Int? = resolvePageSize(pageSize, default = 100)

    override fun pageSize(): Int? = pageSize

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

    // No buildRowNumberedSubquery override: rrnSelectExpression below gives Native2SQL a direct
    // RRN expression, so it never needs to fall back to the ROW_NUMBER()-wrapped-FROM technique
    // (that fallback, and its "native ORDER BY inside OVER()" override, is DefaultSQLDialect-only
    // territory now - see Native2SQLAdapter.tableExpr/directRrnExpr).

    // Convention column: the external table-creation process is expected to declare `__RNN` as
    // `GENERATED ALWAYS AS IDENTITY PRIMARY KEY` on every migrated table - a real, stable,
    // monotonic identity value assigned once at insert time, as close to a persistent physical
    // position as a relational table can offer. Reload does not compute it, only projects it.
    // This is an unconditional contract, not defensively checked per-table: every table reload's
    // PostgreSQLDialect touches is expected to have been migrated to declare it (see
    // rrn-output-support-reload.md's open risk #2) - SQLDBTestUtils.createFile's Postgres branch
    // declares it accordingly (TypedMetadata.toSQL's isPostgres parameter).
    override fun rrnSelectExpression(tableAlias: String): String = "$tableAlias.\"__RNN\""

    // __RNN is a real bigint column, and reload's whole binding pipeline sends every parameter as
    // a string (see rrnParameterPlaceholder's kdoc) - without this cast PostgreSQL rejects the
    // comparison outright ("operator does not exist: bigint = character varying").
    override fun rrnParameterPlaceholder(): String = "CAST(? AS BIGINT)"
}
