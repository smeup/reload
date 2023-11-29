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

import com.ibm.as400.access.AS400
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.jt400.JT400DBMManager
import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.model.DecimalType
import com.smeup.dbnative.model.VarcharType
import com.smeup.dbnative.utils.TypedField
import com.smeup.dbnative.utils.TypedMetadata
import com.smeup.dbnative.utils.fieldByType
import org.junit.Assert

const val EMPLOYEE_TABLE_NAME = "EMPLOYEE"
const val XEMP2_VIEW_NAME = "XEMP2"
const val TSTTAB_TABLE_NAME = "TSTTAB"
const val TST2TAB_TABLE_NAME = "TST2TAB"
const val MUNICIPALITY_TABLE_NAME = "MUNIC0000B"
const val TEST_LOG = false
//do not change defaultValue
//if you want to create sqlconnection against another db use function: dbManagerForTest(testSQLDBType: TestSQLDBType)
private var defaultDbType = TestSQLDBType.DB2_400
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
            user = "USER",
            password = "*********"),
        //force no create connection for dba operations
        dbaConnectionConfig = null
    )

}

fun dbManagerForTest() = dbManagerForTest(defaultDbType)

fun dbManagerForTest(testSQLDBType: TestSQLDBType) : JT400DBMManager {
    testLog("Creating SQLDBManager with db type = $testSQLDBType")

    val dbManager = JT400DBMManager(testSQLDBType.connectionConfig)
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

fun createAndPopulateMunicipalityTable(dbManager: JT400DBMManager?) {
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
        "src/test/resources/csv/Municipality.csv"
    )
     /**/
}

fun createAndPopulateTestTable(dbManager: JT400DBMManager?) {
    val fields = listOf(
        "TSTFLDCHR"   fieldByType CharacterType(3),
        "TSTFLDNBR"   fieldByType DecimalType(5, 2)
    )

    val keys = listOf(
        "TSTFLDCHR",
        "TSTFLDNBR"
    )


    //createAndPopulateTable( /* ci mette tantissimo >20min, la teniamo fissa già creata */
    registerTable(
        dbManager,
        TSTTAB_TABLE_NAME,
        "TSTREC",
        fields,
        keys,
        "src/test/resources/csv/TstTab.csv"
    )
    /**/
}

fun createAndPopulateTest2Table(dbManager: JT400DBMManager?) {
    val fields = listOf(
        "TSTFLDCHR"   fieldByType CharacterType(3),
        "TSTFLDNBR"   fieldByType DecimalType(5, 2),
        "DESTST"   fieldByType CharacterType(10),
    )

    val keys = listOf(
        "TSTFLDCHR",
        "TSTFLDNBR"
    )


    //createAndPopulateTable( /* ci mette tantissimo >20min, la teniamo fissa già creata */
    registerTable(
        dbManager,
        TST2TAB_TABLE_NAME,
        "TSTREC",
        fields,
        keys,
        "src/test/resources/csv/TstTab.csv"
    )
    /**/
}

fun createAndPopulateEmployeeTable(dbManager: JT400DBMManager?) {
    val fields = listOf(
        "EMPNO"   fieldByType CharacterType(6),
        "FIRSTNME"   fieldByType CharacterType(20),
        "MIDINIT"   fieldByType CharacterType(1),
        "LASTNAME"   fieldByType CharacterType(20),
        "WORKDEPT"   fieldByType CharacterType(3),
    )

    val keys = listOf(
        "EMPNO"
    )


    //createAndPopulateTable( /* ci mette tantissimo >20min, la teniamo fissa già creata */
    registerTable(
        dbManager,
        EMPLOYEE_TABLE_NAME,
        "TSTREC",
        fields,
        keys,
        "src/test/resources/csv/Employee.csv"
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
    dbManager: JT400DBMManager?,
    tableName: String,
    formatName: String,
    fields: List<TypedField>,
    keys: List<String>,
    dataFilePath: String
) {
    val metadata = TypedMetadata(tableName, formatName, fields, keys).fileMetadata()
    dbManager!!.registerMetadata(metadata, true)
    Assert.assertTrue(dbManager.existFile(tableName))
}

/*
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
*/

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
/*
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
 */


