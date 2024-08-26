package com.smeup.dbnative.sql

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.model.Field
import java.lang.Exception


class Native2SQLPerf(val fileMetadata: FileMetadata) {
    private var lastReadInstruction: ReadInstruction? = null
    private var lastPositioningInstruction: PositioningInstruction? = null

    /**
     * Build values replacements for store procedures settings empty numerics values as 0
     */
    private fun buildReplacements(values: List<String>): MutableList<String> {
        return values.mapIndexed { index, s ->
            s.ifEmpty {
                if (isNumeric(fileMetadata.fileKeys[index])) {
                    "0"
                } else {
                    ""
                }
            }
        }.toMutableList()
    }


    private fun isNumeric(fieldName: String): Boolean {
        val field = fileMetadata.fields.find { it.name == fieldName }
        return field?.numeric ?: false
    }

    private fun checkPositioning() {
        requireNotNull(lastPositioningInstruction) {
            "No positioning instruction found"
        }
    }

    private fun checkKeys(keys: List<String>) {
        require(fileMetadata.fileKeys.size > 0) {
            "No keys specified in metadata"
        }
        require(keys.size <= fileMetadata.fileKeys.size) {
            "Number of metadata keys $fileMetadata.fileKeys less than number of positioning/read keys $keys"
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
            val keyname = fileMetadata.fileKeys.get(index)
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
        return fileMetadata.fileKeys.joinToString(
            prefix = "ORDER BY ",
            separator = ", "
        ) { "\"$it\" ${sortOrder.symbol}" }
    }

    fun getReadSqlStatement(): Pair<String, List<String>> {
        checkPositioning()
        return Pair(
            getSQL(
                fileMetadata.fields,
                fileMetadata.fileKeys.subList(0, lastPositioningInstruction!!.keys.size),
                Comparison.EQ,
                fileMetadata.tableName
            ), lastPositioningInstruction!!.keys
        )
    }

    fun getSQLStatement(): Pair<String, List<String>> {
        when (lastReadInstruction!!.method) {
            ReadMethod.CHAIN -> {
                checkReadKeys()
                return Pair(
                    getSQL(
                        fileMetadata.fields,
                        fileMetadata.fileKeys.subList(0, lastReadInstruction!!.keys.size),
                        Comparison.EQ,
                        fileMetadata.tableName
                    ), lastReadInstruction!!.keys
                )
            }

            ReadMethod.READ -> {
                checkRead()
                return getReadCoherentSql(true)
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

    private fun getReadCoherentSql(fullUnion: Boolean = false): Pair<String, List<String>> {
        val columns = fileMetadata.fields.joinToString(", ") { "\"${it.name}\"" }
        val tableName = "\"${fileMetadata.tableName}\""
        val queries = mutableListOf<String>()
        val replacements = mutableListOf<String>()

        lastPositioningInstruction?.let {
            val conditions = it.keys.joinToString(" AND ") { key ->
                "\"${fileMetadata.fileKeys[it.keys.indexOf(key)]}\" >= ?"
            }

            queries.add("SELECT $columns FROM $tableName WHERE $conditions")
            replacements.addAll(buildReplacements(it.keys))
        } ?: run {
            return Pair("SELECT $columns FROM $tableName", replacements)
        }

        return Pair(queries.joinToString(" ") + " " + getSQLOrderByClause(), replacements)
    }

    private fun getCoherentSql(fullUnion: Boolean = false): Pair<String, List<String>> {
        val queries = mutableListOf<String>()
        var replacements = mutableListOf<String>()
        val comparison = getComparison()

        if (lastPositioningInstruction == null) {
            var columns = ""
            fileMetadata.fields.forEachIndexed { index, k ->
                run {
                    columns += "\"" + k.name + "\", "
                }
            }
            var value = ""
            fileMetadata.fileKeys.forEachIndexed { index, k ->
                run {
                    value += "\"" + k + "\" " + Comparison.EQ.symbol + " ? AND "
                }
            }
            replacements.addAll(buildReplacements(lastReadInstruction!!.keys))

            return Pair(
                "SELECT " + columns.removeSuffix(", ") + "FROM \"${fileMetadata.tableName}\" WHERE " + value.removeSuffix(
                    " AND "
                ), replacements
            )
        } else {
            if (lastPositioningInstruction!!.keys.size == 1) {
                queries.add(
                    getSQL(
                        fileMetadata.fields,
                        fileMetadata.fileKeys.subList(0, 1),
                        comparison.first,
                        fileMetadata.tableName
                    )
                )
                replacements.addAll(buildReplacements(lastPositioningInstruction!!.keys))
            } else {
                val limit = if (fullUnion) 1 else 2
                for (i in lastPositioningInstruction!!.keys.size downTo limit) {
                    queries.add(
                        getSQL(
                            fileMetadata.fields,
                            fileMetadata.fileKeys.subList(0, i),
                            if (i == lastPositioningInstruction!!.keys.size) comparison.first else comparison.second,
                            fileMetadata.tableName
                        )
                    )
                    replacements.addAll(buildReplacements(lastPositioningInstruction!!.keys.subList(0, i)))
                }
            }
        }
        return Pair(queries.joinToString(" UNION ") + " " + getSQLOrderByClause(), replacements)
    }
}

private fun getSQL(fields: List<Field>, keys: List<String>, comparison: Comparison, tableName: String): String {

    val columns = fields.joinToString(", ") { "\"${it.name}\"" }

    val conditions = keys.mapIndexed { index, key ->
        "\"$key\" ${if (index < keys.size - 1) Comparison.EQ.symbol else comparison.symbol} ?"
    }.joinToString(" AND ")

    return "SELECT $columns FROM \"$tableName\" WHERE $conditions"
}


fun main() {
    var fields = listOf(Field("Regione", ""), Field("Provincia", ""), Field("Comune", ""))
    var fieldsKeys = listOf("Regione", "Provincia", "Comune")
    var metadata = FileMetadata("test", "rld_comuni", fields, fieldsKeys)
    val adapter = Native2SQLPerf(metadata).apply {
        setPositioning(PositioningMethod.SETLL, listOf("Lombardia", "Brescia", "Erbusco"))
        println(isCoherent(listOf("Lombardia", "Brescia", "Erbusco")))
        setRead(ReadMethod.READE, listOf("Lombardia", "Brescia", "Erbusco"))
    }
    adapter.getSQLStatement().let {
        println(it.first)
        println(it.second)
    }
}
