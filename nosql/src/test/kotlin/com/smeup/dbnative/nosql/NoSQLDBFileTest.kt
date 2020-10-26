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
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.model.DecimalType
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.nosql.utils.dbManagerForTest
import com.smeup.dbnative.utils.fieldByType
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class NoSQLDBFileTest {

    private val tableName = "TSTTAB01"

    private lateinit var dbManager : NoSQLDBMManager
    private var dbfile: DBFile? = null

    @Before
    fun initEnv() {
        dbManager = dbManagerForTest()

        // Create file
        val fields = listOf(
            "TSTFLDCHR" fieldByType CharacterType(3),
            "TSTFLDNBR" fieldByType DecimalType(5, 2)
        )

        val keys = listOf(
           "TSTFLDCHR"
        )

        val metadata = FileMetadata(tableName, "TSTREC", fields, keys)
        dbManager.createFile(metadata)
        assertTrue(dbManager.existFile(tableName))

        // Test metadata serialization/deserialization
        val read_metadata = dbManager.metadataOf(tableName)

        assertTrue(read_metadata == metadata)

        dbfile = dbManager.openFile(tableName)
        dbfile!!.write(Record(RecordField("TSTFLDCHR", "XXX"), RecordField("TSTFLDNBR", "123.45")))
        dbManager.closeFile(tableName)
    }

    @Test
    fun testChain() {
        // Search not existing record
        dbfile = dbManager.openFile(tableName)
        assertTrue(dbfile!!.chain("XYZ").record.isEmpty())

        // Search existing record and test contained fields
        val chainResult = dbfile!!.chain("XXX")
        assertEquals(2, chainResult.record.size)
        val fieldsIterator = chainResult.record.iterator()
        assertEquals("XXX", fieldsIterator.next().value)
        assertEquals("123.45", fieldsIterator.next().value)

        dbManager.closeFile(tableName)
    }

    @Test
    fun testRead() {
        // Search not existing record
        dbfile = dbManager.openFile(tableName)
        assertFalse(dbfile!!.setll("XYZ"))

        // Search existing record and test contained fields
        val chainResult = dbfile!!.setll("XXX")
        assertTrue(chainResult)

        val readResult1 = dbfile!!.read()
        assertTrue(readResult1.record.isNotEmpty())

        val readResult2 = dbfile!!.read()
        assertTrue(readResult2.record.isNotEmpty())

        dbManager.closeFile(tableName)
    }

    @After
    fun destroyEnv() {

    }

}

