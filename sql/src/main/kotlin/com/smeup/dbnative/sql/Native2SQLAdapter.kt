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

/** Synthetic column name standing in for a row's Relative Record Number when [Native2SQL] is in
 *  RRN mode (i.e. `fileMetadata.fileKeys` is empty). Never a real column in the physical table. */
private const val RRN_COLUMN = "RRN__"

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

    /** FROM-clause target: the real table, or (RRN mode) a [SQLDialect.buildRowNumberedSubquery]
     *  numbering every row by [rrnOrderingColumns], aliased back to the table name so every
     *  existing caller that expects a plain quoted table-name expression keeps working unmodified. */
    private fun tableExpr(): String =
        if (rrnMode) {
            val realColumns = fileMetadata.fields.joinToString(", ") { "\"${it.name}\"" }
            val subquery = dialect.buildRowNumberedSubquery(
                realColumns, "\"${fileMetadata.tableName}\"", rrnOrderingColumns, RRN_COLUMN
            )
            "$subquery \"${fileMetadata.tableName}\""
        } else {
            "\"${fileMetadata.tableName}\""
        }

    /** Outer SELECT column list: the RPG-visible fields, plus (RRN mode) the synthetic RRN
     *  column so callers (page-resume, key-match) can read the current row's RRN back out. */
    private fun outerColumns(): String =
        (fileMetadata.fields.map { "\"${it.name}\"" } + if (rrnMode) listOf("\"$RRN_COLUMN\"") else emptyList())
            .joinToString(", ")

    private fun checkPositioning() {
        requireNotNull(lastPositioningInstruction) {
            "No positioning instruction found"
        }
    }

    private fun checkKeys(keys: List<String>) {
        if (rrnMode) {
            require(rrnOrderingColumns.isNotEmpty()) {
                "Cannot perform a Relative Record Number access on unkeyed file '${fileMetadata.name}' " +
                    "(table \"${fileMetadata.tableName}\"): no primary key, unique index, or ordering " +
                    "view found to derive a deterministic row order for RRN. Define one of these on the " +
                    "table, or declare explicit keys in the file's metadata."
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
        return effectiveKeys.joinToString(
            prefix = "ORDER BY ",
            separator = ", "
        ) { "\"$it\" ${sortOrder.symbol}" }
    }

    private fun buildDialectPositioningSQL(columns: String, tableName: String, forward: Boolean): Pair<String, List<String>> {
        val inst = lastPositioningInstruction!!
        val conditions = dialect.buildPositioningConditions(
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
        return Pair(
            getSQL(
                outerColumns(),
                effectiveKeys.subList(0, lastPositioningInstruction!!.keys.size),
                Comparison.EQ,
                tableExpr()
            ), lastPositioningInstruction!!.keys
        )
    }

    fun getSQLStatement(): Pair<String, List<String>> {
        when (lastReadInstruction!!.method) {
            ReadMethod.CHAIN -> {
                checkReadKeys()
                return Pair(
                    getSQL(
                        outerColumns(),
                        effectiveKeys.subList(0, lastReadInstruction!!.keys.size),
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
            effectiveKeys.forEachIndexed { index, k ->
                run {
                    value += "\"" + k + "\" " + Comparison.EQ.symbol + " ? AND "
                }
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

private fun getSQL(columns: String, keys: List<String>, comparison: Comparison, fromClause: String): String {

    val conditions = keys.mapIndexed { index, key ->
        "\"$key\" ${if (index < keys.size - 1) Comparison.EQ.symbol else comparison.symbol} ?"
    }.joinToString(" AND ")

    return "SELECT $columns FROM $fromClause WHERE $conditions"
}


fun main() {
    var fields = listOf(Field("Regione", ""), Field("Provincia", ""), Field("Comune", ""))
    var fieldsKeys = listOf("Regione", "Provincia", "Comune")
    var metadata = FileMetadata("test", "rld_comuni", fields, fieldsKeys)
    val adapter = Native2SQL(metadata).apply {
        setPositioning(PositioningMethod.SETLL, listOf("Lombardia", "Brescia", "Erbusco"))
        println(isCoherent(listOf("Lombardia", "Brescia", "Erbusco")))
        setRead(ReadMethod.READE, listOf("Lombardia", "Brescia", "Erbusco"))
    }
    adapter.getSQLStatement().let {
        println(it.first)
        println(it.second)
    }
}
