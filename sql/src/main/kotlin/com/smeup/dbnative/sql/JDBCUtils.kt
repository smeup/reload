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

package com.smeup.dbnative.sql

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.model.*
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

const val CONVENTIONAL_INDEX_SUFFIX = "_INDEX"

fun ResultSet.joinToString(separator: String = " - "): String {
    val sb = StringBuilder()
    while (this.next()) {
        for (i in 1..this.metaData.columnCount) {
            sb.append("${this.metaData.getColumnName(i)}: ${this.getObject(i)}")
            if (i != this.metaData.columnCount) sb.append(separator)
        }
        sb.appendLine()
    }
    return sb.toString()
}

fun PreparedStatement.bind(values: List<Any>) {
    values.forEachIndexed {
        i, value -> this.setObject(i + 1, value)
    }
}

fun Connection.recordFormatName(tableName: String): String? =
    this.metaData.getTables(null, null, tableName, null).use {
        if (it.next()) {
            val remarks = it.getString("REMARKS")
            if (!remarks.isNullOrBlank()) {
                return@use remarks
            }
        }
        return@use tableName
    }

private fun ResultSet.indexId() = "${this.getString("TABLE_CAT")}." +
        "${this.getString("TABLE_SCHEM")}.${this.getString("INDEX_NAME")}"

private fun ResultSet.isUnique() = this.getInt("NON_UNIQUE") == 0

fun Connection.primaryKeys(tableName: String): List<String> {
    val result = mutableListOf<String>()
    this.metaData.getPrimaryKeys(null, null, tableName).use {
        while (it.next()) {
            result.add(it.getString("COLUMN_NAME"))
        }
    }
    //if primary key is not defined i will get it by first unique index
    if (result.isEmpty()) {
        var indexId: String? = null
        //a row for every field in the indexes
        this.metaData.getIndexInfo(null, null, tableName, true, false).use {
            while (it.next()) {
                if (indexId == null) {
                    indexId = it.indexId()
                }
                if (it.indexId() != indexId) {
                    break
                }
                else {
                    result.add(it.getString("COLUMN_NAME"))
                }
            }
        }
    }
    return result
}

fun Connection.orderingFields(tableName: String): List<String> {
    val result = mutableListOf<String>()
    val statement = "SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = ?"
    val field = "VIEW_DEFINITION"
    this.prepareStatement(statement).use { it ->
        it.setString(1, tableName)
        it.executeQuery().use {
            if (it.next()) {
                // TODO handle DESC and ASC keywords
                val fields = it.getString(field).toUpperCase().substringAfter("ORDER BY").split(",")
                result.addAll(fields.map { fl: String -> fl.substring(fl.lastIndexOf('.') + 1).trim('`', ' ')  })
            }
        }
    }

    return result
}

fun ResultSet?.closeIfOpen() {
    if (this != null) {
        try {
            this.close()
        } catch (t: Throwable) {}
    }
}

fun ResultSet?.hasRecords() = !(!this?.isBeforeFirst!! && this.row == 0)

fun ResultSet?.toValues(): Record {
    if (this != null && this.next()) {
        return this.currentRecordToValues()
    }
    return Record()
}

fun ResultSet?.currentRecordToValues(): Record {
    // TODO create a unit test for the isAfterLast condition
    if (this == null || this.isAfterLast) {
        return Record()
    }
    val result = Record()
    val metadata = this.metaData
    for (i in 1..metadata.columnCount) {
        val value = this.getString(i)
        result.add(RecordField(metadata.getColumnName(i), value))
    }
    return result
}



