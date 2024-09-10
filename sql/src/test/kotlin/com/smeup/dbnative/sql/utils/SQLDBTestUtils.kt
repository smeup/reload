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

package com.smeup.dbnative.sql.utils

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingLevel
import com.smeup.dbnative.model.*
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.sql.CONVENTIONAL_INDEX_SUFFIX
import com.smeup.dbnative.sql.SQLDBMManager
import com.smeup.dbnative.utils.TypedField
import com.smeup.dbnative.utils.TypedMetadata
import com.smeup.dbnative.utils.fieldByType
import com.smeup.dbnative.utils.fieldList
import org.junit.Assert
import java.io.File
import java.sql.Connection
import java.sql.ResultSet
import java.util.*

const val EMPLOYEE_TABLE_NAME = "EMPLOYEE"
const val EMPLOYEE_VIEW_NAME = "EMPLOYEE_VIEW"
const val MUNICIPALITY_TABLE_NAME = "MUNICIPALITY"
const val TEST_LOG = false
private val LOGGING_LEVEL = LoggingLevel.ALL

//do not change defaultValue
//if you want to create sqlconnection against another db use function: dbManagerForTest(testSQLDBType: TestSQLDBType)
private var defaultDbType = if (System.getenv("CI").toBoolean()) {
    when(System.getenv("SQL_DB_TYPE").toString()) {
        "postgresql" -> TestSQLDBType.POSTGRES
        "mysql" -> TestSQLDBType.MY_SQL
        else -> TestSQLDBType.HSQLDB
    }
} else {
    TestSQLDBType.HSQLDB
}
//private var defaultDbType = TestSQLDBType.POSTGRES
//private var defaultDbType = TestSQLDBType.MY_SQL
//private var defaultDbType = TestSQLDBType.DB2_400
const val DATABASE_NAME = "TEST"
const val DB2_400_HOST = "SRVLAB01.SMEUP.COM"
const val DB2_400_LIBRARY_NAME = "UP_PRR"

enum class TestSQLDBType(
    val connectionConfig: ConnectionConfig,
    val dbaConnectionConfig: ConnectionConfig? = connectionConfig,
    val createDatabase: (dbaConnection: Connection) -> Unit = {},
    val destroyDatabase: (dbaConnection: Connection) -> Unit = {}
) {
    MY_SQL(
        connectionConfig = ConnectionConfig(
            fileName = "*",
            url = "jdbc:mysql://localhost:3306/$DATABASE_NAME",
            user = "root",
            password = "root"
        ),
        dbaConnectionConfig = ConnectionConfig(
            fileName = "*",
            url = "jdbc:mysql://localhost:3306/",
            user = "root",
            password = "root"
        ),
        createDatabase = { dbaConnection ->
            dbaConnection.prepareStatement("CREATE DATABASE IF NOT EXISTS $DATABASE_NAME").use { it.execute() }
        },
        destroyDatabase = { dbaConnection ->
            dbaConnection.prepareStatement("DROP DATABASE IF EXISTS  $DATABASE_NAME").use { it.execute() }
        }
    ),
    HSQLDB(ConnectionConfig(
        fileName = "*",
        url = "jdbc:hsqldb:mem:$DATABASE_NAME",
        user = "sa",
        password = "root"
    ),
        destroyDatabase = { dbaConnection ->
            dbaConnection.prepareStatement("DROP SCHEMA PUBLIC CASCADE").use { it.execute() }
        }
    ),

    // Added H2 embedded DB with postgres compability (for tests)
    H2SQLDB(ConnectionConfig(
        fileName = "*",
        url = "jdbc:h2:~/$DATABASE_NAME;MODE=PostgreSQL",
        user = "sa",
        password = ""
    ),
        destroyDatabase = { dbaConnection ->
            dbaConnection.prepareStatement("DROP SCHEMA PUBLIC CASCADE").use { it.execute() }
        }
    ),
    DB2_400(
        ConnectionConfig(
            fileName = "*",
            driver = "com.ibm.as400.access.AS400JDBCDriver",
            url = "jdbc:as400://$DB2_400_HOST/$DB2_400_LIBRARY_NAME;",
            user = System.getenv("AS400_USR") ?: "USER",
            password = System.getenv("AS400_PWD") ?: "PASSWORD"
        ),

        //force no create connection for dba operations
        dbaConnectionConfig = null
    ),
    DB2_400_DAT(
        ConnectionConfig(
            fileName = "*",
            driver = "com.ibm.as400.access.AS400JDBCDriver",
            url = "jdbc:as400://$DB2_400_HOST/SMEUP_DAT",
            user = "USER",
            password = "**********"
        ),

        //force no create connection for dba operations
        dbaConnectionConfig = null
    ),
    POSTGRES(
        ConnectionConfig(
            fileName = "*",
            url = "jdbc:postgresql://localhost:5432/postgres",
            user = "root",
            password = "root",
            driver = "org.postgresql.Driver"
        )
    )

}

fun dbManagerDB2400ForTest(host: String, library: String): SQLDBMManager {
    val dbManager = SQLDBMManager(
        ConnectionConfig(
            fileName = "*",
            driver = "com.ibm.as400.access.AS400JDBCDriver",
            url = "jdbc:as400://$host/$library;",
            user = System.getenv("AS400_USR") ?: "USER",
            password = System.getenv("AS400_PWD") ?: "PASSWORD"
        ),
    )
    dbManager.logger = Logger.getSimpleInstance(LOGGING_LEVEL)
    return dbManager
}

fun dbManagerForTest() = dbManagerForTest(defaultDbType)

fun dbManagerForTest(testSQLDBType: TestSQLDBType): SQLDBMManager {
    println("Creating SQLDBManager with db type = $testSQLDBType")

    val dbManager = SQLDBMManager(testSQLDBType.connectionConfig)
    if (testSQLDBType.dbaConnectionConfig != null) {
        SQLDBMManager(testSQLDBType.dbaConnectionConfig).connection.use {
            testSQLDBType.createDatabase(it)
        }
    }
    dbManager.setSQLLog(TEST_LOG)
    dbManager.logger = Logger.getSimpleInstance(LOGGING_LEVEL)
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

fun createAndPopulateEmployeeTable(dbManager: SQLDBMManager?) {
    val fields = listOf(
        "EMPNO" fieldByType CharacterType(6),
        "FIRSTNME" fieldByType VarcharType(12),
        "MIDINIT" fieldByType VarcharType(1),
        "LASTNAME" fieldByType VarcharType(15),
        "WORKDEPT" fieldByType CharacterType(3)
    )

    val keys = listOf(
        "EMPNO"
    )

    createAndPopulateTable(
        dbManager,
        EMPLOYEE_TABLE_NAME,
        EMPLOYEE_TABLE_NAME,
        fields,
        keys,
        "src/test/resources/csv/Employee.csv"
    )
}

fun destroyView() {
    destroyView(defaultDbType)
}

fun destroyView(testSQLDBType: TestSQLDBType) {
    if (testSQLDBType.dbaConnectionConfig != null) {
        SQLDBMManager(testSQLDBType.dbaConnectionConfig).connection.use {
            it.prepareStatement("DROP VIEW IF EXISTS \"$EMPLOYEE_VIEW_NAME\"")
        }
    }
}

fun destroyIndex() {
    destroyIndex(defaultDbType)
}

fun destroyIndex(testSQLDBType: TestSQLDBType) {
    if (testSQLDBType.dbaConnectionConfig != null) {
        SQLDBMManager(testSQLDBType.dbaConnectionConfig).connection.use {
            it.prepareStatement("DROP INDEX IF EXISTS \"$EMPLOYEE_VIEW_NAME$CONVENTIONAL_INDEX_SUFFIX\"")
        }
    }
}

fun createAndPopulateEmployeeView(dbManager: SQLDBMManager?) {
    // create view executing sql -> TODO: insert a createView method in DBMManager and use it
    fun createXEMP2() = "CREATE VIEW \"$EMPLOYEE_VIEW_NAME\" AS SELECT * FROM \"$EMPLOYEE_TABLE_NAME\" ORDER BY \"WORKDEPT\", \"EMPNO\""

    fun createEmployeeIndex() =
        "CREATE INDEX \"$EMPLOYEE_VIEW_NAME$CONVENTIONAL_INDEX_SUFFIX\" ON \"$EMPLOYEE_TABLE_NAME\" (\"WORKDEPT\" ASC, \"EMPNO\" ASC)"

    val fields = listOf(
        "EMPNO" fieldByType CharacterType(6),
        "FIRSTNME" fieldByType VarcharType(12),
        "MIDINIT" fieldByType VarcharType(1),
        "LASTNAME" fieldByType VarcharType(15),
        "WORKDEPT" fieldByType CharacterType(3)
    )

    val keys = listOf(
        "WORKDEPT"
    )

    val metadata = FileMetadata(EMPLOYEE_VIEW_NAME, EMPLOYEE_TABLE_NAME, fields.fieldList(), keys)
    dbManager!!.registerMetadata(metadata, true)
    try {
        dbManager.execute(listOf(createXEMP2(), createEmployeeIndex()))
    } catch (e: Exception){
        println(e)
    }}



fun createAndPopulateMunicipalityTable(dbManager: SQLDBMManager?) {
    val fields = listOf(
        "NAZ" fieldByType CharacterType(2),
        "REG" fieldByType CharacterType(3),
        "PROV" fieldByType CharacterType(2),
        "CITTA" fieldByType VarcharType(35),
        "CAP" fieldByType CharacterType(5),
        "PREF" fieldByType CharacterType(4),
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
        MUNICIPALITY_TABLE_NAME,
        fields,
        keys,
        "src/test/resources/csv/Municipality.csv"
    )
}

fun getEmployeeName(record: Record): String {
    return record["FIRSTNME"].toString() + " " + record["LASTNAME"].toString()
}

fun getMunicipalityName(record: Record): String {
    return (record["CITTA"]?.trim() ?: "")
}

fun getMunicipalityProv(record: Record): String {
    return (record["PROV"]?.trim() ?: "")
}

fun testLog(message: String) {
    if (TEST_LOG) {
        println(message)
    }
}

private fun createAndPopulateTable(
    dbManager: SQLDBMManager?,
    name: String,
    tableName: String,
    fields: List<TypedField>,
    keys: List<String>,
    dataFilePath: String
) {

    val tMetadata = TypedMetadata(name, tableName, fields, keys)
    createFile(tMetadata, dbManager!!)
    Assert.assertTrue(dbManager.existFile(tableName))
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
    } catch (e: Exception) {
        println(e)
    }

    dbManager.closeFile(tableName)
}

fun buildMunicipalityKey(vararg values: String): List<String> {
    val keyValues = mutableListOf<String>()
    val keys = arrayOf("NAZ", "REG", "PROV", "CITTA")
    for ((index, value) in values.withIndex()) {
        if (keys.size > index) {
            keyValues.add(value)
        }
    }
    return keyValues
}

fun buildCountryKey(vararg values: String): List<String> {
    val keyValues = mutableListOf<String>()
    val keys = arrayOf("NAZ", "REG", "PROV")
    for ((index, value) in values.withIndex()) {
        if (keys.size > index) {
            keyValues.add(value)
        }
    }
    return keyValues
}

fun buildRegionKey(vararg values: String): List<String> {
    val keyValues = mutableListOf<String>()
    val keys = arrayOf("NAZ", "REG")
    for ((index, value) in values.withIndex()) {
        if (keys.size > index) {
            keyValues.add(value)
        }
    }
    return keyValues
}

fun buildNationKey(vararg values: String): List<String> {
    val keyValues = mutableListOf<String>()
    val keys = arrayOf("NAZ")
    for ((index, value) in values.withIndex()) {
        if (keys.size > index) {
            keyValues.add(value)
        }
    }
    return keyValues
}

fun createFile(tMetadata: TypedMetadata, dbManager: SQLDBMManager) {
    val metadata: FileMetadata = tMetadata.fileMetadata()
    dbManager.connection.createStatement().use {
        println(tMetadata.toSQL())
        it.execute(tMetadata.toSQL())
    }
    dbManager.registerMetadata(metadata, true)
}

fun TypedMetadata.toSQL(): String = "CREATE TABLE IF NOT EXISTS \"${this.tableName}\" (${this.fields.toSQL(this)})"


fun Collection<TypedField>.toSQL(tMetadata: TypedMetadata): String {
    val primaryKeys = tMetadata.fileKeys.joinToString { "\"$it\"" }

    return joinToString { "\"${it.field.name}\" ${it.type2sql()}" } + (if (primaryKeys.isEmpty()) "" else ", PRIMARY KEY($primaryKeys)")
}

fun TypedField.type2sql(): String =
    when (this.type.type) {
        Type.CHARACTER -> "CHAR(${this.type.size}) DEFAULT '' NOT NULL"
        Type.VARCHAR -> "VARCHAR(${this.type.size}) DEFAULT '' NOT NULL"
        Type.INTEGER -> "INT"
        Type.SMALLINT -> "SMALLINT"
        Type.BIGINT -> "BIGINT"
        Type.BOOLEAN -> "BOOLEAN"
        Type.DECIMAL -> "DECIMAL(${this.type.size},${this.type.digits}) DEFAULT 0 NOT NULL"
        Type.FLOAT -> "FLOAT(${this.type.size},${this.type.digits}) DEFAULT 0 NOT NULL"
        Type.DOUBLE -> "DOUBLE DEFAULT 0 NOT NULL"
        Type.TIMESTAMP -> "TIMESTAMP"
        Type.TIME -> "TIME"
        Type.DATE -> "DATE"
        Type.BINARY -> "BINARY"
        Type.VARBINARY -> "VARBINARY(${this.type.size})"
        else -> TODO("Conversion to SQL Type not yet implemented: ${this.type}")
    }


fun sql2Type(metadataResultSet: ResultSet): FieldType {
    val sqlType = metadataResultSet.getString("TYPE_NAME")
    val columnSize = metadataResultSet.getInt("COLUMN_SIZE")
    val decimalDigits = metadataResultSet.getInt("DECIMAL_DIGITS")
    return sql2Type(sqlType, columnSize, decimalDigits)
}


/**
 * Convert SQL type in FieldType
 */
fun sql2Type(sqlType: String, columnSize: Int, decimalDigits: Int): FieldType =
    when (sqlType) {
        "CHAR", "CHARACTER", "NCHAR" -> CharacterType(columnSize)
        "VARCHAR" -> VarcharType(columnSize)
        "INT", "INTEGER" -> IntegerType
        "SMALLINT" -> SmallintType
        "BIGINT" -> BigintType
        "BOOLEAN", "BOOL" -> BooleanType
        "DECIMAL", "NUMERIC" -> DecimalType(columnSize, decimalDigits)
        "DOUBLE" -> DoubleType
        "FLOAT" -> FloatType
        "TIMESTAMP" -> TimeStampType
        "TIME" -> TimeType
        "DATE" -> DateType
        "BINARY" -> BinaryType(columnSize)
        "VARBINARY" -> VarbinaryType(columnSize)
        else -> TODO("Conversion from SQL Type not yet implemented: $sqlType")
    }