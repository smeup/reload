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

package com.smeup.dbnative.nosql

import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.nosql.utils.TSTAB_TABLE_NAME
import com.smeup.dbnative.nosql.utils.createAndPopulateTestTable
import com.smeup.dbnative.nosql.utils.dbManagerForTest
import org.junit.*
import org.junit.Assert.assertTrue
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class NoSQLDBFileTest {

    private val tableName = TSTAB_TABLE_NAME

    companion object {
        private lateinit var dbManager : NoSQLDBMManager

        @BeforeClass
        @JvmStatic
        fun initEnv() {
            dbManager = dbManagerForTest()
            createAndPopulateTestTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dbManager.close()
        }
    }

    @Test
    fun testChain() {
        // Search not existing record
        val dbfile: DBFile = dbManager.openFile(tableName)
        assertTrue(dbfile.chain("XYZ").record.isEmpty())

        // Search existing record and test contained fields
        val chainResult = dbfile.chain("XXX")
        assertEquals(2, chainResult.record.size)
        val fieldsIterator = chainResult.record.iterator()
        assertEquals("XXX", fieldsIterator.next().value)
        assertEquals("123.45", fieldsIterator.next().value)

        dbManager.closeFile(tableName)
    }

    @Test
    fun testRead() {
        // Search not existing record
        val dbfile: DBFile = dbManager.openFile(tableName)
        assertFalse(dbfile.setll("XYZ"))

        // Search existing record and test contained fields
        val chainResult = dbfile.setll("XXX")
        assertTrue(chainResult)

        val readResult1 = dbfile.read()
        assertTrue(readResult1.record.isNotEmpty())

        dbManager.closeFile(tableName)
    }

    @After
    fun destroyEnv() {

    }

}

