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

import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.metadata.file.PropertiesSerializer
import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class DB2400OperationsOnFilePerfTest {


    private var dbManager: SQLDBMManager? = null

    fun initDbManager(host: String = DB2_400_HOST, library: String = DB2_400_LIBRARY_NAME) {
        dbManager = dbManagerDB2400ForTest(host, library)
    }

    @Test
    fun insert() {
        initDbManager(library = "UP_PRR")
        dbManager.use {
            val fileMetadata =
                PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG3L")
            it!!.registerMetadata(fileMetadata, false)
            val dbFile = it!!.openFile("VERAPG3L")

            var record = Record(RecordField("V£IDOJ", "A3L00000X1"),
                RecordField("V£DATA", "20210117"),
                RecordField("V£NOME", "BUSFIO2        "),
                RecordField("V£CDC", "SMEGL.001      "),
                RecordField("V£COD1", "ERB            "))
            dbFile.write(record)


            it!!.closeFile("VERAPG3L")
        }
    }

    @Test
    fun delete() {
        initDbManager(library = "UP_PRR")
        dbManager.use {
            val fileMetadata =
                PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG0L")
            it!!.registerMetadata(fileMetadata, false)
            val dbFile = it!!.openFile("VERAPG0L")

            var record = dbFile.chain(arrayListOf("A3L00000X1")).record
            if (!dbFile.eof()) {
                dbFile.delete(record)
            }
            it!!.closeFile("VERAPG0L")
        }
    }

    @Ignore
    @Test
    fun updateMethods() {
        initDbManager(library = "UP_PRR")
        dbManager.use {
            val fileMetadata =
                PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG0L")
            it!!.registerMetadata(fileMetadata, false)
            val dbFile = it!!.openFile("VERAPG0L")

            var record = dbFile.chain(arrayListOf("A3L0000001")).record
            if (dbFile.eof()) {
                record = Record(RecordField("V£IDOJ", "A3L0000001"),
                    RecordField("V£DATA", "20210117"),
                    RecordField("V£NOME", "BUSFIO         "),
                    RecordField("V£CDC", "SMEGL.001      "),
                    RecordField("V£COD1", "ERB            "))
                dbFile.write(record)
                record = dbFile.chain(arrayListOf("A3L0000001")).record
                assertFalse { dbFile.eof() }
            }

            record.put("V£ATV0", "2")
            record.put("V£COM2", "\$\$EXT   Prova aggiornamento       EXT\$\$")
            dbFile.update(record)

            record = dbFile.chain(arrayListOf("A3L0000001")).record
            assertEquals(record["V£ATV0"], "2")
            dbFile.delete(record)

            it!!.closeFile("VERAPG3L")
        }
    }

    @Test
    fun setllReadeNoMatch() {
        initDbManager(library = "XSMEDATGRU")
        dbManager.use {
            val fileMetadata =
                PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG9L")
            it!!.registerMetadata(fileMetadata, false)
            val dbFile = it!!.openFile("VERAPG9L")
            var keys = arrayListOf("20210117", "SMEGL.001      ")
            dbFile.setll(keys)
            dbFile.readEqual(keys)
            it!!.closeFile("VERAPG9L")
        }
    }

    @Test
    fun chain() {
        initDbManager(library = "SMEUP_DAT")
        dbManager.use {
            val fileMetadata =
                PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI2L")
            it!!.registerMetadata(fileMetadata, false)
            val dbFile = it!!.openFile("BRARTI2L")
            var keys = arrayListOf("ART  ", "A08            ")
            for (i in 1..10) {
                doChain(keys, dbFile)
            }
            keys = arrayListOf("ART  ", "MS01           ")
            for (i in 1..10) {
                doChain(keys, dbFile)
            }
            keys = arrayListOf("ART  ", "SL03           ")
            for (i in 1..10) {
                doChain(keys, dbFile)
            }
            keys = arrayListOf("ART  ", "TX01           ")
            for (i in 1..10) {
                doChain(keys, dbFile)
            }
            keys = arrayListOf("ART  ", "MAN            ")
            for (i in 1..10) {
                doChain(keys, dbFile)
            }
            it!!.closeFile("BRARTI0L")
        }
    }

    private fun doChain(keys: List<String>, dbFile: DBFile){
        val chainResult = dbFile.chain(keys)
        assertEquals(keys[1], chainResult.record["A§ARTI"])
    }


}

