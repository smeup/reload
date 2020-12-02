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

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.jt400.JT400DBMMAnager
import com.smeup.dbnative.sql.SQLDBMManager

private var defaultDbType = TestSQLDBType.DB2_400_JTOPEN
private var sqldbmManager : SQLDBMManager? = null

fun dbManagerForTest() : JT400DBMMAnager {

    sqldbmManager = com.smeup.dbnative.jt400.utils.dbManagerForTest(TestSQLDBType.DB2_400)
    return JT400DBMMAnager(defaultDbType.connectionConfig)
    /*
    testLog("Creating SQLDBManager with db type = $testSQLDBType")

    val dbManager = SQLDBMManager(testSQLDBType.connectionConfig)
    if (testSQLDBType.dbaConnectionConfig != null) {
        SQLDBMManager(testSQLDBType.dbaConnectionConfig).connection.use {
            testSQLDBType.createDatabase(it)
        }
    }
    dbManager.setSQLLog(TEST_LOG)
    return dbManager

     */
}

fun createAndPopulateEmployeeTable(dbManager: JT400DBMMAnager?) {
    com.smeup.dbnative.jt400.utils.createAndPopulateEmployeeTable(sqldbmManager)
    dbManager!!.registerMetadata(sqldbmManager!!.metadataOf(EMPLOYEE_TABLE_NAME), true)
}

fun createAndPopulateXemp2View(dbManager: JT400DBMMAnager?) {
    com.smeup.dbnative.jt400.utils.createAndPopulateXemp2View(sqldbmManager)
    dbManager!!.registerMetadata(sqldbmManager!!.metadataOf(XEMP2_VIEW_NAME), true)
}

fun destroyDatabase() {
    com.smeup.dbnative.jt400.utils.destroyDatabase(TestSQLDBType.DB2_400)
}

fun getEmployeeName(record: Record): String {
    return record["FIRSTNME"].toString() + " " + record["LASTNAME"].toString()
}
