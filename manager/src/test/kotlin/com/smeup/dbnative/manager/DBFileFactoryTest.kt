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

package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingLevel
import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.sql.SQLDBMManager
import com.smeup.dbnative.utils.TypedField
import com.smeup.dbnative.utils.fieldByType
import com.smeup.dbnative.utils.fieldList
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertTrue
private val LOGGING_LEVEL = LoggingLevel.OFF

private enum class TestConnectionConfig(
    val connectionConfig: ConnectionConfig,
    val dbaConnectionConfig: ConnectionConfig = connectionConfig,
    val createDatabase : (dbaConnection: Connection) -> Unit = {},
    val destroyDatabase: (dbaConnection: Connection) -> Unit = {}) {
    DEFAULT(
        connectionConfig = ConnectionConfig(
            fileName= "*",
            url = "jdbc:hsqldb:mem:TEST",
            user = "",
            password = "",
            driver = "org.hsqldb.jdbcDriver"
        )
    ),
    STARTS_WITH_TEST(
        connectionConfig = ConnectionConfig(
            fileName= "TEST*",
            url = "jdbc:hsqldb:mem:TEST",
            user = "",
            password = "",
            driver = "org.hsqldb.jdbcDriver"
        )
    ),
    MUNICIPALITY(ConnectionConfig(
        fileName= "MUNICIPALITY",
        url = "mongodb://localhost:27017/W_TEST",
        user = "",
        password = ""))
}

class DBFileFactoryTest {

    private lateinit var config : DBNativeAccessConfig
    private lateinit var manager: SQLDBMManager

    @Before
    fun setUp() {
        config = DBNativeAccessConfig(mutableListOf(
            TestConnectionConfig.DEFAULT.connectionConfig,
            TestConnectionConfig.STARTS_WITH_TEST.connectionConfig,
            TestConnectionConfig.MUNICIPALITY.connectionConfig
        ),
        Logger.getSimpleInstance(LOGGING_LEVEL))
        manager = SQLDBMManager(TestConnectionConfig.STARTS_WITH_TEST.connectionConfig)
        manager.connection.createStatement().use {
            it.executeUpdate("CREATE TABLE TEST1 (NAME CHAR(20))")
            it.executeUpdate("INSERT INTO TEST1 VALUES ('MARCO')")
            it.executeUpdate("CREATE TABLE TEST2 (NAME CHAR(20))")
            it.executeUpdate("INSERT INTO TEST2 VALUES ('MARCO')")
            it.executeUpdate("CREATE TABLE TEST3 (NAME CHAR(20))")
            it.executeUpdate("INSERT INTO TEST3 VALUES ('MARCO')")
        }

        var testFields = mutableListOf<TypedField>()
        testFields.add("Name" fieldByType CharacterType(20))
        val testTableMetadata = FileMetadata("TEST1", "TSTFFMT", testFields.fieldList(), listOf("Name"))
        manager.registerMetadata(testTableMetadata, true)


    }

    @Test
    fun findConnectionForPIPPO() {
        assertEquals(TestConnectionConfig.DEFAULT.connectionConfig,
            findConnectionConfigFor("PIPPO/PLUTO", config.connectionsConfig))
    }

    @Test
    fun findConnectionForTESTXXX() {
        assertEquals(TestConnectionConfig.STARTS_WITH_TEST.connectionConfig,
            findConnectionConfigFor("TEST3", config.connectionsConfig))
    }

    @Test
    fun findConnectionForMUNICIPALITY() {
        assertEquals(TestConnectionConfig.MUNICIPALITY.connectionConfig,
            findConnectionConfigFor("MUNICIPALITY", config.connectionsConfig))
    }

    //test ok if no throw exception
    @Test
    fun openExistingTables() {
        DBFileFactory(config).use {dbFileFactory ->
            // Open a file already registered
            dbFileFactory.open("TEST1", null)

            // Open a file not registered, registering metadata before open
            var test2Fields = mutableListOf<TypedField>()
            test2Fields.add("Name" fieldByType CharacterType(20))
            val test2TableMetadata = FileMetadata("TEST2", "TSTFFMT", test2Fields.fieldList(), listOf<String>())
            DBFileFactory.registerMetadata(test2TableMetadata)

            dbFileFactory.open("TEST2", null)

            // Open a file not registered, passing metadata to open invoke
            var test3Fields = mutableListOf<TypedField>()
            test3Fields.add("Name" fieldByType CharacterType(20))
            val test3TableMetadata = FileMetadata("TEST3", "TSTFFMT", test2Fields.fieldList(), listOf("Name"))
            dbFileFactory.open("TEST3", test3TableMetadata)
        }
    }

    @Test
    fun openNotExistingTables() {
        DBFileFactory(config).use { dbFileFactory ->
            assertTrue(dbFileFactory.runCatching { open("MARIOLINA", null) }.isFailure)
        }
    }

    @Test
    fun reopenClosedFile() {
        DBFileFactory(config).use {dbFileFactory ->
            val dbFile = dbFileFactory.open("TEST1", null)
            dbFile.read()
            dbFile.close()
            assertTrue (dbFile.runCatching { read() }.isFailure)
        }
    }

    @After
    fun tearDown() {
        manager.runCatching {
            connection.createStatement().use {
                it.executeUpdate("DROP TABLE TEST1")
                it.executeUpdate("DROP TABLE TEST2")
                it.executeUpdate("DROP TABLE TEST3")
            }
        }
        manager.close()

        manager.unregisterMetadata("TEST1")
        manager.unregisterMetadata("TEST2")
        manager.unregisterMetadata("TEST3")
    }

}