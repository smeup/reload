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

package com.smeup.reload.sql.utils

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.smeup.reload.ConnectionConfig
import com.smeup.reload.file.Record
import com.smeup.reload.file.RecordField
import com.smeup.reload.model.*
import com.smeup.reload.sql.CONVENTIONAL_INDEX_SUFFIX
import com.smeup.reload.sql.SQLDBMManager
import com.smeup.reload.utils.fieldByType
import org.junit.Assert
import java.io.File
import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger

const val EMPLOYEE_TABLE_NAME = "EMPLOYEE"
const val XEMP2_VIEW_NAME = "XEMP2"
const val TSTTAB_TABLE_NAME = "TSTTAB"
const val TST2TAB_TABLE_NAME = "TSTTAB"
const val MUNICIPALITY_TABLE_NAME = "MUNICIPALITY"
const val TEST_LOG = false
//do not change defaultValue
//if you want to create sqlconnection against another db use function: dbManagerForTest(testSQLDBType: TestSQLDBType)
private var defaultDbType = TestSQLDBType.HSQLDB
//private var defaultDbType = TestSQLDBType.MY_SQL
//private var defaultDbType = TestSQLDBType.DB2_400
const val DATABASE_NAME = "TEST"
const val DB2_400_HOST = "SRVLAB01.SMEUP.COM"
const val DB2_400_LIBRARY_NAME = "W_PARFRA"

enum class TestSQLDBType(
    val connectionConfig: ConnectionConfig,
    val dbaConnectionConfig: ConnectionConfig? = connectionConfig,
    val createDatabase : (dbaConnection: Connection) -> Unit = {},
    val destroyDatabase: (dbaConnection: Connection) -> Unit = {}) {
    MY_SQL(
        connectionConfig = ConnectionConfig(
            fileName= "*",
            url = "jdbc:mysql://localhost:3306/$DATABASE_NAME",
            user = "root",
            password = "root"),
        dbaConnectionConfig = ConnectionConfig(
            fileName= "*",
            url = "jdbc:mysql://localhost:3306/",
            user = "root",
            password = "root"),
        createDatabase = { dbaConnection -> dbaConnection.prepareStatement("CREATE DATABASE $DATABASE_NAME").use { it.execute() }  },
        destroyDatabase = { dbaConnection -> dbaConnection.prepareStatement("DROP DATABASE  $DATABASE_NAME").use { it.execute() }  }
    ),
    HSQLDB(ConnectionConfig(
        fileName= "*",
        url = "jdbc:hsqldb:mem:$DATABASE_NAME",
        user = "sa",
        password = "root"),
        destroyDatabase = { dbaConnection -> dbaConnection.prepareStatement("DROP SCHEMA PUBLIC CASCADE").use { it.execute() }  }
    ),
    DB2_400(ConnectionConfig(
            fileName= "*",
            driver = "com.ibm.as400.access.AS400JDBCDriver",
            url = "jdbc:as400://$DB2_400_HOST/$DB2_400_LIBRARY_NAME;",
            user = "PARFRA",
            password = ""),
        //force no create connection for dba operations
        dbaConnectionConfig = null
    )

}

object DatabaseNameFactory {
    var COUNTER = AtomicInteger()
}

fun dbManagerForTest() = dbManagerForTest(defaultDbType)

fun dbManagerForTest(testSQLDBType: TestSQLDBType) : SQLDBMManager {
    testLog("Creating SQLDBManager with db type = $testSQLDBType")

    val dbManager = SQLDBMManager(testSQLDBType.connectionConfig)
    if (testSQLDBType.dbaConnectionConfig != null) {
        SQLDBMManager(testSQLDBType.dbaConnectionConfig).connection.use {
            testSQLDBType.createDatabase(it)
        }
    }
    dbManager.setSQLLog(TEST_LOG)
    return dbManager
}

fun destroyDatabase() {
    destroyDatabase(defaultDbType)
}

fun destroyDatabase(testSQLDBType: TestSQLDBType) {
    if (testSQLDBType.dbaConnectionConfig != null) {
        SQLDBMManager(testSQLDBType.dbaConnectionConfig).connection.use {
            testSQLDBType.destroyDatabase(it)
        }
    }
}

fun createAndPopulateTstTable(dbManager: SQLDBMManager?) {
    val fields = listOf(
        "TSTFLDCHR" fieldByType CharacterType(3),
        "TSTFLDNBR" fieldByType DecimalType(5, 2)
    )

    val keys = listOf(
        "TSTFLDCHR"
    )


    createAndPopulateTable(dbManager, TSTTAB_TABLE_NAME, "TSTREC", fields, keys, false, "src/test/resources/csv/TstTab.csv")
}

fun createAndPopulateTst2Table(dbManager: SQLDBMManager?) {
    val fields = listOf(
        "TSTFLDCHR" fieldByType VarcharType(3),
        "TSTFLDNBR" fieldByType DecimalType(5, 2),
        "DESTST" fieldByType VarcharType(40)
    )

    val keys = listOf(
        "TSTFLDCHR",
        "TSTFLDNBR"
    )

    createAndPopulateTable(dbManager, TST2TAB_TABLE_NAME, "TSTREC", fields, keys, false,"src/test/resources/csv/Tst2Tab.csv")
}

fun createAndPopulateEmployeeTable(dbManager: SQLDBMManager?) {
    val fields = listOf(
        "EMPNO"     fieldByType CharacterType(6),
        "FIRSTNME"  fieldByType VarcharType(12),
        "MIDINIT"   fieldByType VarcharType(1),
        "LASTNAME"  fieldByType VarcharType(15),
        "WORKDEPT"  fieldByType CharacterType(3)
    )

    val keys = listOf(
        "EMPNO"
    )

    createAndPopulateTable(dbManager, EMPLOYEE_TABLE_NAME, "TSTREC", fields, keys, false,"src/test/resources/csv/Employee.csv")
}

fun createAndPopulateXemp2View(dbManager: SQLDBMManager?) {
    // create view executing sql -> TODO: insert a createView method in DBMManager and use it
    fun createXEMP2() = "CREATE VIEW $XEMP2_VIEW_NAME AS SELECT * FROM EMPLOYEE ORDER BY WORKDEPT, EMPNO"

    fun createXEMP2Index() =
        "CREATE INDEX $XEMP2_VIEW_NAME$CONVENTIONAL_INDEX_SUFFIX ON EMPLOYEE (WORKDEPT ASC, EMPNO ASC)"

    val fields = listOf(
        "EMPNO"     fieldByType CharacterType(6),
        "FIRSTNME"  fieldByType VarcharType(12),
        "MIDINIT"   fieldByType VarcharType(1),
        "LASTNAME"  fieldByType VarcharType(15),
        "WORKDEPT"  fieldByType CharacterType(3)
    )

    val keys = listOf(
        "WORKDEPT"
    )

    val metadata = FileMetadata("$XEMP2_VIEW_NAME", "EMPLOYEE", fields, keys, false)
    dbManager!!.registerMetadata(metadata, false)
    dbManager.execute(listOf(createXEMP2(), createXEMP2Index()))
}

fun createAndPopulateMunicipalityTable(dbManager: SQLDBMManager?) {
    val fields = listOf(
        "NAZ"   fieldByType CharacterType(2),
        "REG"   fieldByType CharacterType(3),
        "PROV"  fieldByType CharacterType(2),
        "CITTA" fieldByType VarcharType(35),
        "CAP"   fieldByType CharacterType(5),
        "PREF"  fieldByType CharacterType(4),
        "COMUNE" fieldByType CharacterType(4),
        "ISTAT" fieldByType CharacterType(6)
    )

    val keys = listOf(
        "NAZ",
        "REG",
        "PROV",
        "CITTA"
    )


    createAndPopulateTable(
        dbManager,
        MUNICIPALITY_TABLE_NAME,
        "TSTREC",
        fields,
        keys,
        false,
        "src/test/resources/csv/Municipality.csv"
    )
}

fun getEmployeeName(record: Record): String {
    return record["FIRSTNME"].toString() + " " + record["LASTNAME"].toString()
}

fun getMunicipalityName(record: Record): String {
    return (record["CITTA"]?.toString()?.trim() ?: "")
}

fun testLog(message: String) {
    if (TEST_LOG) {
        println(message)
    }
}

private fun createAndPopulateTable(
    dbManager: SQLDBMManager?,
    tableName: String,
    formatName: String,
    fields: List<Field>,
    keys: List<String>,
    unique: Boolean,
    dataFilePath: String
) {

    val metadata = FileMetadata(tableName, formatName, fields, keys, unique)
    dbManager!!.createFile(metadata)
    Assert.assertTrue(dbManager.existFile(tableName))
    dbManager.registerMetadata(metadata, true)
    val dbFile = dbManager.openFile(tableName)

    val dataFile = File(dataFilePath)
    val dataRows: List<Map<String, String>> = csvReader().readAllWithHeader(dataFile)

    dataRows.forEach {
        val recordFields = emptyList<RecordField>().toMutableList()
        it.map { (key, value) ->
            recordFields.add(RecordField(key, value))
        }
        dbFile.write(Record(*recordFields.toTypedArray()))
    }

    dbManager.closeFile(tableName)
}

fun buildMunicipalityKey(vararg values: String): List<RecordField> {
    val recordFields = mutableListOf<RecordField>()
    val keys = arrayOf("NAZ", "REG", "PROV", "CITTA")
    for ((index, value) in values.withIndex()) {
        if (keys.size> index) {
            recordFields.add(RecordField(keys[index], value))
        }
    }
    return recordFields
}


