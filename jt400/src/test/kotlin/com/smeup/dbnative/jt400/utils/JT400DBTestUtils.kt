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

package com.smeup.dbnative.jt400.utils

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.ibm.as400.access.AS400
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.jt400.JT400DBMMAnager
import com.smeup.dbnative.model.*
//import com.smeup.dbnative.jt400.CONVENTIONAL_INDEX_SUFFIX
//import com.smeup.dbnative.jt400.JT400DBMMAnager
import com.smeup.dbnative.utils.fieldByType
import org.junit.Assert
import java.io.File
import java.lang.Exception
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

const val EMPLOYEE_TABLE_NAME = "EMPLOYEE"
const val XEMP2_VIEW_NAME = "XEMP2"
const val TSTTAB_TABLE_NAME = "TSTTAB"
const val TST2TAB_TABLE_NAME = "TST2TAB"
const val MUNICIPALITY_TABLE_NAME = "MUNIC0000B"
const val TEST_LOG = false
//do not change defaultValue
//if you want to create sqlconnection against another db use function: dbManagerForTest(testSQLDBType: TestSQLDBType)
private var defaultDbType = TestSQLDBType.DB2_400
const val DATABASE_NAME = "TEST"
const val DB2_400_HOST = "SRVLAB01.SMEUP.COM"
const val DB2_400_LIBRARY_NAME = "W_PARFRA"

const val CONVENTIONAL_INDEX_SUFFIX = "_INDEX"

enum class TestSQLDBType(
    val connectionConfig: ConnectionConfig,
    val dbaConnectionConfig: ConnectionConfig? = connectionConfig,
    val createDatabase : (dbaConnection: AS400) -> Unit = {},
    val destroyDatabase: (dbaConnection: AS400) -> Unit = {}) {
    DB2_400(ConnectionConfig(
            fileName= "*",
            driver = "com.ibm.as400.access.AS400JDBCDriver",
            url = "jdbc:as400://$DB2_400_HOST/$DB2_400_LIBRARY_NAME;",
            user = "SCAARM",
            password = "**********"),
        //force no create connection for dba operations
        dbaConnectionConfig = null
    )

}

object DatabaseNameFactory {
    var COUNTER = AtomicInteger()
}

fun dbManagerForTest() = dbManagerForTest(defaultDbType)

fun dbManagerForTest(testSQLDBType: TestSQLDBType) : JT400DBMMAnager {
    testLog("Creating SQLDBManager with db type = $testSQLDBType")

    val dbManager = JT400DBMMAnager(testSQLDBType.connectionConfig)
    if (testSQLDBType.dbaConnectionConfig != null) {
        //JT400DBMMAnager(testSQLDBType.dbaConnectionConfig).connection.use {
        //    testSQLDBType.createDatabase(it)
        //}
    }
    //dbManager.setSQLLog(TEST_LOG)
    return dbManager
}

fun destroyDatabase() {
    destroyDatabase(defaultDbType)
}

fun destroyDatabase(testSQLDBType: TestSQLDBType) {
    if (testSQLDBType.dbaConnectionConfig != null) {
        //JT400DBMMAnager(testSQLDBType.dbaConnectionConfig).connection.use {
        //    testSQLDBType.destroyDatabase(it)
        //}
    }
}

fun createAndPopulateTstTable(dbManager: JT400DBMMAnager?) {
    val fields = listOf(
        "TSTFLDCHR" fieldByType CharacterType(3),
        "TSTFLDNBR" fieldByType DecimalType(5, 2)
    )

    val keys = listOf(
        "TSTFLDCHR"
    )


    createAndPopulateTable(dbManager, TSTTAB_TABLE_NAME, "TSTREC", fields, keys, false, "src/test/resources/csv/TstTab.csv")
}

fun createAndPopulateTst2Table(dbManager: JT400DBMMAnager?) {
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

fun createAndPopulateEmployeeTable(dbManager: JT400DBMMAnager?) {
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

    //createAndPopulateTable(
    registerTable(
        dbManager, EMPLOYEE_TABLE_NAME, "TSTREC", fields, keys, false,"src/test/resources/csv/Employee.csv")
}

fun createAndPopulateXemp2View(dbManager: JT400DBMMAnager?) {
    // create view executing sql -> TODO: insert a createView method in DBMManager and use it
    fun createXEMP2() = "CREATE VIEW $XEMP2_VIEW_NAME AS SELECT * FROM (SELECT * FROM EMPLOYEE ORDER BY WORKDEPT, EMPNO)"

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
    dbManager!!.registerMetadata(metadata, true)
    execute(listOf(createXEMP2(), createXEMP2Index()))
}

fun createAndPopulateMunicipalityTable(dbManager: JT400DBMMAnager?) {
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


    //createAndPopulateTable( /* ci mette tantissimo >20min, la teniamo fissa già creata */
    registerTable(
        dbManager,
        MUNICIPALITY_TABLE_NAME,
        "TSTREC",
        fields,
        keys,
        false,
        "src/test/resources/csv/Municipality.csv"
    )
     /**/
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

private fun registerTable(
    dbManager: JT400DBMMAnager?,
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
}

private fun createAndPopulateTable(
    dbManager: JT400DBMMAnager?,
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

    try {
        dataRows.forEach {
            val recordFields = emptyList<RecordField>().toMutableList()
            it.map { (key, value) ->
                recordFields.add(RecordField(key, value))
            }
            dbFile.write(Record(*recordFields.toTypedArray()))
        }
    } catch (e : Exception) {
        println(e.message)
    }

    dbManager.closeFile(tableName)
}

fun buildMunicipalityKey(vararg values: String): List<String> {
    val recordFields = mutableListOf<String>()
    val keys = arrayOf("NAZ", "REG", "PROV", "CITTA")
    for ((index, value) in values.withIndex()) {
        if (keys.size> index) {
            recordFields.add(value)
        }
    }
    return recordFields
}

fun execute(sqlStatements: List<String>) {
    val connection = connectJDBC();
    connection.createStatement().use { statement ->
        sqlStatements.forEach {
                sql ->
            println(sql)
            statement.addBatch(sql)
        }
        statement.executeBatch()
    }
    connection.close();
}

fun connectJDBC() : Connection {
    val connectionConfig = TestSQLDBType.DB2_400.connectionConfig;

    connectionConfig.driver?.let {
        Class.forName(connectionConfig.driver)
    }
    return DriverManager.getConnection(connectionConfig.url, connectionConfig.user, connectionConfig.password)
}


