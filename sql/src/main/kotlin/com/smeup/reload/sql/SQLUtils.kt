/*
 * Copyright 2020 The Reload project Authors
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

package com.smeup.reload.sql

import com.smeup.reload.file.Record
import com.smeup.reload.file.RecordField
import com.smeup.reload.model.Field
import com.smeup.reload.model.FileMetadata

fun FileMetadata.toSQL(): String = "CREATE TABLE ${this.tableName} (${this.fields.toSQL(this)})"


private fun Collection<Field>.toSQL(fileMetadata: FileMetadata): String {
    val primaryKeys = fileMetadata.fileKeys.joinToString { it }

    return joinToString { "${it.name} ${it.type2sql()}" } + (if (primaryKeys.isEmpty()) "" else ", PRIMARY KEY($primaryKeys)")
}

fun String.insertSQL(record: Record): String {
    val names = record.keys.joinToString { it }
    val questionMarks = record.keys.joinToString { "?" }
    return "INSERT INTO $this ($names) VALUES($questionMarks)"
}

fun String.updateSQL(record: Record): String {
    val namesAndQuestionMarks = record.keys.joinToString { "$it = ?" }
    val wheres = record.keys.toList()
    val comparations = List(record.size) { Comparison.EQ }
    return "UPDATE $this SET $namesAndQuestionMarks ${whereSQL(wheres, comparations)}"
}

fun String.deleteSQL(record: Record): String {
    val wheres = record.keys.toList()
    val comparations = List(record.size) { Comparison.EQ }
    return "DELETE FROM $this ${whereSQL(wheres, comparations)}"
}

fun orderBySQL(keysNames: List<String>, reverse: Boolean = false): String =
    if (keysNames.isEmpty()) {
        ""
    } else {
        if (reverse) {
            "ORDER BY " + keysNames.joinToString(separator = " DESC, ", postfix = " DESC")
        } else {
            "ORDER BY " + keysNames.joinToString()
        }
    }

fun whereSQL(wheres: List<String>, comparations: List<Comparison>): String {
    return if (wheres.isEmpty()) {
        ""
    } else {
        var result = "WHERE "
        wheres.forEachIndexed { index, _ -> result += wheres[index] + " " + comparations[index].symbol + " ? AND " }
        return result.removeSuffix("AND ")
    }
}

fun filePartSQLAndValues(
    tableName: String,
    movingForward: Boolean,
    fileKeys: List<String>,
    keys: List<RecordField>,
    withEquals: Boolean
): Pair<MutableList<String>, String> {

    val queries = mutableListOf<String>()
    val values = mutableListOf<String>()

    var keys2 = keys
    val keys2Size = keys2.size

    do {

        var lastComparison = if (movingForward) {
            Comparison.GT
        } else {
            Comparison.LT
        }

        val comparisons = mutableListOf<Comparison>()
        keys2.forEachIndexed { index, _ ->
            if (index < keys2.size - 1) {
                comparisons.add(Comparison.EQ)
            } else {
                if (keys2.size == keys2Size && withEquals) {
                    if (lastComparison == Comparison.GT) {
                        lastComparison = Comparison.GE
                    } else if (lastComparison == Comparison.LT) {
                        lastComparison = Comparison.LE
                    }
                }
                comparisons.add(lastComparison)
            }
        }

        val sql = "(SELECT * FROM $tableName ${whereSQL(
            keys2.map { it.name },
            comparisons
        )})"

        values.addAll(keys2.map { it.value as String })
        queries.add(sql)
        keys2 = keys2.subList(0, keys2.size - 1)

    } while (keys2.isNotEmpty())

    val sql = queries.joinToString(" UNION ") + " " + orderBySQL(
        fileKeys,
        reverse = !movingForward
    )

    return Pair(values, sql)
}

enum class Comparison(val symbol: String) {
    EQ("="),
    NE("<>"),
    GT(">"),
    GE(">="),
    LT("<"),
    LE("<=");
}

fun createMarkerSQL(keysNames: List<String>): String =
    if (keysNames.isEmpty()) {
        ""
    } else {
        // in HSQLDB CONCAT needs at least two params!
        if (keysNames.size == 1) {
            "CONCAT( " + keysNames.joinToString() + ", '') AS NATIVE_ACCESS_MARKER"
        } else {
            "CONCAT( " + keysNames.joinToString() + ") AS NATIVE_ACCESS_MARKER"
        }

    }

fun calculateMarkerValue(
    keys: List<RecordField>,
    movingForward: Boolean = true,
    withEquals: Boolean = true
): String =
    if (keys.isEmpty()) {
        ""
    } else {
        val padChar = if (movingForward) {
            ' '
        } else {
            'Z'
        }
        // NOTE: calculate max length of marker using primary fields max length (temp 100 but incorrect)
        if (withEquals) {
            keys.joinToString("") { it.value as String}.padEnd(100, padChar)
        } else {
            keys.joinToString("") { it.value as String}

        }
    }

fun markerWhereSQL(movingForward: Boolean, withEquals: Boolean): String {
    val comparison = if (movingForward && !withEquals) {
        Comparison.GT
    } else if (movingForward && withEquals) {
        Comparison.GE
    } else if (!movingForward && !withEquals) {
        Comparison.LT
    } else {
        Comparison.LE
    }
    return " WHERE NATIVE_ACCESS_MARKER ${comparison.symbol} ? "
}






