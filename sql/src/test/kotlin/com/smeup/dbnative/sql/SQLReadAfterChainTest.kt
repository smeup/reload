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

import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * READ/READP right after a CHAIN, and [com.smeup.dbnative.file.DBFile.resetPosition].
 * IBM i semantics followed (not yet proved on IBM i): after a successful CHAIN, READ returns the record that follows
 * the chained one and READP the one that precedes it; after a CLOSE/OPEN the first READ returns the first record.
 */
class SQLReadAfterChainTest {

    companion object {

        private lateinit var dbManager: SQLDBMManager

        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateEmployeeTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dbManager.close()
            destroyDatabase()
        }
    }

    private fun empNo(record: com.smeup.dbnative.file.Record) = record["EMPNO"].toString().trim()

    @Test
    fun readAfterChainReturnsTheFollowingRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        assertEquals("000030", empNo(dbFile.chain("000030").record))
        assertEquals("000050", empNo(dbFile.read().record))
        assertEquals("000060", empNo(dbFile.read().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun readPreviousAfterChainReturnsThePrecedingRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        assertEquals("000030", empNo(dbFile.chain("000030").record))
        assertEquals("000020", empNo(dbFile.readPrevious().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun chainAfterChainStillWorks() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        dbFile.chain("000030")
        assertEquals("000050", empNo(dbFile.chain("000050").record))
        assertEquals("000060", empNo(dbFile.read().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun resetPositionAfterChainMakesReadStartFromTheFirstRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        dbFile.chain("000030")
        dbFile.resetPosition()
        assertEquals("000010", empNo(dbFile.read().record))
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }

    @Test
    fun resetPositionAfterReadMakesReadStartFromTheFirstRecord() {
        val dbFile = dbManager.openFile(EMPLOYEE_TABLE_NAME)
        dbFile.read()
        dbFile.read()
        dbFile.resetPosition()
        assertEquals("000010", empNo(dbFile.read().record))
        assertTrue(!dbFile.eof())
        dbManager.closeFile(EMPLOYEE_TABLE_NAME)
    }
}
