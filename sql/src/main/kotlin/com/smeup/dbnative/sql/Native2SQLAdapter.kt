package com.smeup.dbnative.sql

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.model.Field
import java.lang.Exception

enum class PositioningMethod {
    SETLL, SETGT;
}

enum class ReadMethod(val forward: Boolean) {
    READE(true), READPE(false), READP(false), READ(true), CHAIN(true);
}

enum class SortOrder(val symbol: String) {
    ASCEDING("ASC"), DESCENDING("DESC");
}

class PositioningInstruction(val method: PositioningMethod, val keys: List<String>) {
    init {
        require(keys.isNotEmpty()) {
            "No keys specified for positioning instruction $method"
        }
    }
}

class ReadInstruction(var method: ReadMethod, var keys: List<String>) {
    constructor(method: ReadMethod) : this(method, emptyList()) {
        require(admitEmptyKeys()) {
            "Keys are mandatory for read instruction $method "
        }
    }

    fun admitEmptyKeys() = method == ReadMethod.READ || method == ReadMethod.READP
}

/** Synthetic column name standing in for a row's Relative Record Number. In RRN mode (i.e.
 *  `fileMetadata.fileKeys` is empty) this is also the key CHAIN/SETLL/SETGT/READE/READPE position
 *  by; on every file, keyed or not, it is additionally projected as an output column so callers
 *  can read back the RRN of whatever row was read (see [Native2SQL.outerColumns]). Never a real
 *  column in the physical table. Internal (not private) so [SQLDBFile] can strip it out of the
 *  RPG-visible [com.smeup.dbnative.file.Record] and read it into [com.smeup.dbnative.file.Result.rrn]
 *  instead. */
internal const val RRN_COLUMN = "RRN__"

class Native2SQL(
    val fileMetadata: FileMetadata,
    private val dialect: SQLDialect = DefaultSQLDialect(),
    /** Columns to order by when deriving a Relative Record Number for an unkeyed file (via
     *  `ROW_NUMBER() OVER (ORDER BY ...)`); ignored when `fileMetadata.fileKeys` is non-empty. */
    private val rrnOrderingColumns: List<String> = emptyList()
) {
    private var lastReadInstruction: ReadInstruction? = null
    private var lastPositioningInstruction: PositioningInstruction? = null

    /** True when this file is unkeyed (arrival-sequence): CHAIN/SETLL/SETGT/READE/READPE
     *  operate by Relative Record Number instead of by key. */
    private val rrnMode: Boolean = fileMetadata.fileKeys.isEmpty()

    /** The column name(s) every key-column-driven code path below should use: the real file
     *  keys, or (RRN mode) the single synthetic RRN column. */
    private val effectiveKeys: List<String> = if (rrnMode) listOf(RRN_COLUMN) else fileMetadata.fileKeys

    /**
     * Build values replacements for store procedures settings empty numerics values as 0
     */
    private fun buildReplacements(values: List<String>): MutableList<String> {
        return values.mapIndexed { index, s ->
            s.ifEmpty {
                if (isNumeric(effectiveKeys[index])) {
                    "0"
                } else {
                    ""
                }
            }
        }.toMutableList()
    }

    private fun isNumeric(fieldName: String): Boolean {
        if (fieldName == RRN_COLUMN) return true
        val field = fileMetadata.fields.find { it.name == fieldName }
        return field?.numeric ?: false
    }

    private fun quotedTableName(): String = "\"${fileMetadata.tableName}\""

    /** [SQLDialect.rrnSelectExpression] for this file's table, but only consulted in RRN mode: a
     *  keyed file's positioning is always by its real key columns, never by RRN, so outside RRN
     *  mode this is always null regardless of what the dialect could offer. Non-null here means
     *  DB2/PostgreSQL-style direct RRN access - no `FROM` restructuring needed for either the
     *  output RRN ([outerColumns]) or query-by-RRN positioning ([keyExpr], [buildDialectPositioningSQL]).
     *  Null means either a keyed file, or RRN mode on a dialect with no direct expression
     *  (Default/HSQLDB), which still needs the [tableExpr] ROW_NUMBER() fallback. */
    private fun directRrnExpr(): String? = if (rrnMode) dialect.rrnSelectExpression(quotedTableName()) else null

    /** FROM-clause target: the real table, or (RRN mode, no [directRrnExpr]) a
     *  [SQLDialect.buildRowNumberedSubquery] numbering every row by [rrnOrderingColumns], aliased
     *  back to the table name so every existing caller that expects a plain quoted table-name
     *  expression keeps working unmodified.
     *
     *  Deliberately NOT extended to keyed files for dialects with no direct [SQLDialect.rrnSelectExpression]
     *  (i.e. [DefaultSQLDialect]/HSQLDB): wrapping FROM in a derived table there would make the
     *  query's `ResultSet` non-updatable, breaking [SQLDBFile.update]/[SQLDBFile.delete] (verified:
     *  HSQLDB's JDBC driver rejects `updateObject`/`deleteRow` on a derived-table query with
     *  "attempt to assign to non-updatable column"). Since HSQLDB is test-only, a keyed read there
     *  simply has no output RRN ([outerColumns] omits the column, so [Native2SQL]'s caller reads
     *  back `Result.rrn == null`), same as any backend with no RRN concept at all. */
    private fun tableExpr(): String =
        if (rrnMode && directRrnExpr() == null) {
            val realColumns = fileMetadata.fields.joinToString(", ") { "\"${it.name}\"" }
            val subquery = dialect.buildRowNumberedSubquery(
                realColumns, quotedTableName(), rrnOrderingColumns, RRN_COLUMN
            )
            "$subquery ${quotedTableName()}"
        } else {
            quotedTableName()
        }

    /** SQL fragment identifying the effective key at [index], for use directly inside a `WHERE`/
     *  `ORDER BY` clause: [directRrnExpr] (RRN mode, DB2/PostgreSQL) used raw/unquoted since it's
     *  already a complete SQL expression rather than a bare identifier; otherwise the quoted
     *  column identifier - the real file key, or (RRN mode, no direct expression) the synthetic
     *  RRN_COLUMN alias produced by [tableExpr]'s row-numbered `FROM` clause. RRN mode is always
     *  single-key (see [checkKeys]), so [index] is only ever meaningful for keyed files. */
    private fun keyExpr(index: Int): String = directRrnExpr() ?: "\"${effectiveKeys[index]}\""

    /** Bound-parameter placeholder to pair with [keyExpr]'s output at the same [index]: a plain
     *  `?` for a real key column, or [SQLDialect.rrnParameterPlaceholder] when [keyExpr] is
     *  [directRrnExpr] - see that placeholder's kdoc for why a direct RRN comparison sometimes
     *  needs one (PostgreSQL's `__RNN` is a real `bigint` column). */
    private fun placeholderFor(index: Int): String = if (directRrnExpr() != null) dialect.rrnParameterPlaceholder() else "?"

    /** True when this query can output an RRN at all: always in RRN mode (either [directRrnExpr]
     *  or the row-numbered FROM clause computes one), or when [dialect] has a direct expression
     *  (DB2, PostgreSQL) that doesn't require restructuring FROM. False only for a keyed file on a
     *  dialect with no direct expression - see [tableExpr]'s doc for why that case is deliberately
     *  excluded. */
    private fun hasOutputRrn(): Boolean = rrnMode || dialect.rrnSelectExpression(quotedTableName()) != null

    /** Outer SELECT column list: the RPG-visible fields, plus (when [hasOutputRrn]) the row's RRN,
     *  so callers (page-resume, key-match, and ordinary reads) can read the current row's RRN back
     *  out. Projected either as [SQLDialect.rrnSelectExpression] appended directly to the SELECT
     *  list (DB2, PostgreSQL - cheap, no FROM/WHERE change, keyed or not), or as the already-computed
     *  synthetic column from the row-numbered FROM clause (RRN mode, no direct expression). */
    private fun outerColumns(): String {
        val fieldColumns = fileMetadata.fields.map { "\"${it.name}\"" }
        if (!hasOutputRrn()) return fieldColumns.joinToString(", ")
        val directExpr = dialect.rrnSelectExpression(quotedTableName())
        val rrnColumn = if (directExpr != null) "$directExpr AS \"$RRN_COLUMN\"" else "\"$RRN_COLUMN\""
        return (fieldColumns + rrnColumn).joinToString(", ")
    }

    private fun checkPositioning() {
        requireNotNull(lastPositioningInstruction) {
            "No positioning instruction found"
        }
    }

    private fun checkKeys(keys: List<String>) {
        if (rrnMode) {
            require(rrnOrderingColumns.isNotEmpty()) {
                "Cannot perform a Relative Record Number access on unkeyed file '${fileMetadata.name}' " +
                    "(table \"${fileMetadata.tableName}\"): no primary key or unique index found on the " +
                    "table, and the file's metadata declares no fields to fall back on for a " +
                    "deterministic row order. Declare at least one field in the file's metadata, or " +
                    "explicit keys, to fix this."
            }
            require(keys.size <= 1) {
                "Relative Record Number access takes at most one positioning/read value (the RRN), got $keys"
            }
        } else {
            require(keys.size <= fileMetadata.fileKeys.size) {
                "Number of metadata keys $fileMetadata.fileKeys less than number of positioning/read keys $keys"
            }
        }
    }

    private fun checkRead() {
        requireNotNull(lastReadInstruction) {
            "No read instruction found"
        }
    }

    private fun checkReadKeys() {
        checkRead()
        require(lastReadInstruction!!.keys.isNotEmpty()) {
            "No keys specified for read instruction ${lastReadInstruction!!.method}"
        }
    }

    private fun checkInstructions() {
        //checkPositioning()
        checkRead()
    }

    fun setPositioning(method: PositioningMethod, keys: List<String>) {
        checkKeys(keys)
        lastPositioningInstruction = PositioningInstruction(method, keys)
        lastReadInstruction = null
    }

    /**
     * Check key list:
     * - If passed key are less then file metadata keys fill missing var with empty values
     * - If a key is numeric and is empty, set its value to 0
     */
    private fun checkEmptyKeys(keys: List<String>): List<String> {
        val result = mutableListOf<String>()

        result.addAll(keys)
        val lostKeys = fileMetadata.fileKeys.size - result.size
        repeat(lostKeys) {
            result.add("")
        }

        val checkedResult = mutableListOf<String>()
        result.forEachIndexed { index, s ->
            if (s.isNullOrEmpty()) {
                if (isNumeric(fileMetadata.fileKeys.get(index))) {
                    checkedResult.add("0")
                } else {
                    checkedResult.add("")
                }
            } else {
                checkedResult.add(s)
            }
        }

        return checkedResult
    }

    /*
     * @return true if read method need new query execution
     */
    fun setRead(method: ReadMethod, keys: List<String>? = null): Boolean {
        checkKeys(keys ?: emptyList())
        var executeQuery = false
        when (method) {
            ReadMethod.READPE, ReadMethod.READE -> {
                val coherent = keys?.let { isCoherent(keys) } ?: true
                //Test to remove on fully supported operations
                require(coherent) {
                    "Uncoherent read not yet managed"
                }
            }

            ReadMethod.READP -> checkPositioning()
            ReadMethod.CHAIN -> {
                lastPositioningInstruction = null
                executeQuery = true
            }
        }
        require(lastReadInstruction == null || method == ReadMethod.CHAIN || method == lastReadInstruction!!.method) {
            "read operation " + method + " is allowed immediatly after positioning or after a same method read instruction"
        }
        if (lastReadInstruction == null) {
            executeQuery = true
        }
        lastReadInstruction = ReadInstruction(method, keys ?: emptyList())
        return executeQuery
    }

    fun clear() {
        lastReadInstruction = null
        lastPositioningInstruction = null
    }

    fun getLastKeys() =
        lastReadInstruction?.keys ?: lastPositioningInstruction?.keys ?: throw Exception("Keys not yet set")

    fun isLastOperationSet() = lastReadInstruction == null

    fun lastReadMatchRecord(record: Record): Boolean {

        if (lastReadInstruction!!.keys.isEmpty()) {
            return true
        }

        lastReadInstruction!!.keys.mapIndexed { index, value ->
            val keyname = effectiveKeys.get(index)
            if (record[keyname]?.trim() != value.trim()) {
                return false
            }
        }
        return true
    }


    fun isCoherent(newKeys: List<String>): Boolean {
        //checkPositioning()
        if (lastPositioningInstruction != null) {
            return newKeys.isEmpty() ||
                    if (newKeys.size <= lastPositioningInstruction!!.keys.size &&
                        newKeys.size <= (lastReadInstruction?.keys?.size ?: newKeys.size)
                    ) {
                        newKeys.forEachIndexed() { index, value ->
                            if (lastPositioningInstruction!!.keys.get(index) != value) {
                                return false
                            }
                        }
                        return true
                    } else false
        } else return true
    }

    private fun getSortOrder(): SortOrder {
        checkInstructions()
        return if (lastReadInstruction!!.method.forward) SortOrder.ASCEDING else SortOrder.DESCENDING
    }

    private fun getComparison(): Pair<Comparison, Comparison> {
        checkInstructions()
        return if (lastPositioningInstruction == null) {
            return Pair(Comparison.EQ, Comparison.GT)
        } else if (lastReadInstruction!!.method.forward) {
            when (lastPositioningInstruction!!.method) {
                PositioningMethod.SETLL -> return Pair(Comparison.EQ, Comparison.GT)
                PositioningMethod.SETGT -> return Pair(Comparison.GT, Comparison.GT)
            }
        } else {
            when (lastPositioningInstruction!!.method) {
                PositioningMethod.SETLL -> return Pair(Comparison.LT, Comparison.LT)
                PositioningMethod.SETGT -> return Pair(Comparison.LE, Comparison.LT)
            }
        }
    }

    private fun getSQLOrderByClause(): String {
        val sortOrder = getSortOrder()
        return effectiveKeys.indices.joinToString(
            prefix = "ORDER BY ",
            separator = ", "
        ) { "${keyExpr(it)} ${sortOrder.symbol}" }
    }

    /** For RRN mode with a [directRrnExpr] only (DB2, PostgreSQL): the single-fragment equivalent
     *  of [SQLDialect.buildPositioningConditions], built directly off the raw expression instead
     *  of a quoted identifier. RRN-mode positioning is always exactly one key (see [checkKeys]),
     *  so the general multi-key UNION strategy [SQLDialect.buildPositioningConditions] exists for
     *  is unneeded here - just one `<expr> <cmp> <placeholder>` fragment. */
    private fun buildRrnDirectPositioningConditions(
        expr: String,
        positioningKey: String,
        method: PositioningMethod,
        forward: Boolean
    ): List<Pair<String, List<String>>> {
        val (cmp, _) = comparisonFor(method, forward)
        return listOf(Pair("$expr ${cmp.symbol} ${dialect.rrnParameterPlaceholder()}", buildReplacements(listOf(positioningKey))))
    }

    private fun buildDialectPositioningSQL(columns: String, tableName: String, forward: Boolean): Pair<String, List<String>> {
        val inst = lastPositioningInstruction!!
        val conditions = directRrnExpr()?.let { expr ->
            buildRrnDirectPositioningConditions(expr, inst.keys[0], inst.method, forward)
        } ?: dialect.buildPositioningConditions(
            effectiveKeys, inst.keys, inst.method, forward, ::buildReplacements
        )
        var sql = conditions.joinToString(" UNION ") { (where, _) ->
            "SELECT $columns FROM $tableName WHERE $where"
        } + " ${getSQLOrderByClause()}"
        dialect.pageSize()?.let { sql += " FETCH FIRST $it ROWS ONLY" }
        return Pair(sql, conditions.flatMap { it.second })
    }

    /**
     * True while a positioning-based query (bounded by [SQLDialect.pageSize] and thus subject to
     * transparent page resume) is the active read path, as opposed to CHAIN or an un-positioned
     * full-table READ, neither of which go through [buildDialectPositioningSQL].
     */
    fun hasPositioning(): Boolean = lastPositioningInstruction != null

    fun pageSize(): Int? = dialect.pageSize()

    /**
     * Builds the SQL to continue a positioning-based scan once its current page (capped by
     * [SQLDialect.pageSize]) is exhausted, seeking strictly past [lastRecord] in the current read
     * direction. Replaces [lastPositioningInstruction] but deliberately leaves [lastReadInstruction]
     * untouched (unlike [setPositioning]) so the caller-visible read cycle (e.g. the original
     * readEqual key filter) is unaffected by this internal repage.
     */
    fun getResumeSqlStatement(lastRecord: Record): Pair<String, List<String>> {
        checkPositioning()
        val forward = lastReadInstruction!!.method.forward
        val resumeMethod = if (forward) PositioningMethod.SETGT else PositioningMethod.SETLL
        val resumeKeys = effectiveKeys.map { lastRecord[it].orEmpty() }
        lastPositioningInstruction = PositioningInstruction(resumeMethod, resumeKeys)
        return buildDialectPositioningSQL(outerColumns(), tableExpr(), forward)
    }

    fun getReadSqlStatement(): Pair<String, List<String>> {
        checkPositioning()
        val n = lastPositioningInstruction!!.keys.size
        return Pair(
            getSQL(
                outerColumns(),
                (0 until n).map { keyExpr(it) },
                (0 until n).map { placeholderFor(it) },
                Comparison.EQ,
                tableExpr()
            ), lastPositioningInstruction!!.keys
        )
    }

    fun getSQLStatement(): Pair<String, List<String>> {
        when (lastReadInstruction!!.method) {
            ReadMethod.CHAIN -> {
                checkReadKeys()
                val n = lastReadInstruction!!.keys.size
                return Pair(
                    getSQL(
                        outerColumns(),
                        (0 until n).map { keyExpr(it) },
                        (0 until n).map { placeholderFor(it) },
                        Comparison.EQ,
                        tableExpr()
                    ), lastReadInstruction!!.keys
                )
            }

            ReadMethod.READ -> {
                checkRead()
                return getReadCoherentSql()
            }

            ReadMethod.READP -> {
                checkPositioning()
                checkRead()
                return getCoherentSql(true)
            }

            else -> {
                //checkPositioning()
                checkReadKeys()
                return getCoherentSql()
            }
        }
    }

    private fun getReadCoherentSql(): Pair<String, List<String>> {
        val columns = outerColumns()
        val tableName = tableExpr()
        lastPositioningInstruction ?: return Pair("SELECT $columns FROM $tableName", emptyList())
        return buildDialectPositioningSQL(columns, tableName, lastReadInstruction!!.method.forward)
    }

    private fun getCoherentSql(fullUnion: Boolean = false): Pair<String, List<String>> {
        val replacements = mutableListOf<String>()

        if (lastPositioningInstruction == null) {
            val columns = outerColumns()
            var value = ""
            effectiveKeys.indices.forEach { index ->
                value += keyExpr(index) + " " + Comparison.EQ.symbol + " " + placeholderFor(index) + " AND "
            }
            replacements.addAll(buildReplacements(lastReadInstruction!!.keys))

            return Pair(
                "SELECT $columns FROM ${tableExpr()} WHERE " + value.removeSuffix(" AND "), replacements
            )
        } else {
            return buildDialectPositioningSQL(outerColumns(), tableExpr(), lastReadInstruction!!.method.forward)
        }
    }
}

/** [keyExprs] are already-complete SQL fragments (quoted identifiers or a raw dialect expression -
 *  see [Native2SQL.keyExpr]), not bare column names, so no further quoting happens here.
 *  [placeholders] pairs a bound-parameter placeholder with each [keyExprs] entry at the same
 *  index - see [Native2SQL.placeholderFor]. */
private fun getSQL(columns: String, keyExprs: List<String>, placeholders: List<String>, comparison: Comparison, fromClause: String): String {

    val conditions = keyExprs.mapIndexed { index, keyExpr ->
        "$keyExpr ${if (index < keyExprs.size - 1) Comparison.EQ.symbol else comparison.symbol} ${placeholders[index]}"
    }.joinToString(" AND ")

    return "SELECT $columns FROM $fromClause WHERE $conditions"
}


fun main() {
    var fields = listOf(Field("Regione", ""), Field("Provincia", ""), Field("Comune", ""))
    var fieldsKeys = listOf("Regione", "Provincia", "Comune")
    var metadata = FileMetadata("test", "rld_comuni", fields, fieldsKeys)
    val adapter = Native2SQL(metadata, rrnOrderingColumns = fieldsKeys).apply {
        setPositioning(PositioningMethod.SETLL, listOf("Lombardia", "Brescia", "Erbusco"))
        println(isCoherent(listOf("Lombardia", "Brescia", "Erbusco")))
        setRead(ReadMethod.READE, listOf("Lombardia", "Brescia", "Erbusco"))
    }
    adapter.getSQLStatement().let {
        println(it.first)
        println(it.second)
    }
}
