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
import com.smeup.dbnative.metadata.file.PropertiesSerializer
import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class DB2400OperationsOnFilePerfTest {


    private var dbManager: SQLDBMManager? = null

    fun initDbManager(host: String = DB2_400_HOST, library: String = DB2_400_LIBRARY_NAME) {
        dbManager = dbManagerDB2400ForTest(host, library)
    }

    @Test
    fun setllReadeNoMatch() {
        initDbManager(library = "XSMEDATGRU")
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "VERAPG9L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("VERAPG9L")
        var keys = arrayListOf("20210117", "SMEGL.001      ")
        dbFile.setll(keys)
        dbFile.readEqual(keys)
        dbManager!!.closeFile("VERAPG9L")
        dbManager!!.close()
    }

    @Test
    fun chain() {
        initDbManager(library = "SMEUP_DAT")
        val fileMetadata = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI2L")
        dbManager!!.registerMetadata(fileMetadata, false)
        val dbFile = dbManager!!.openFile("BRARTI2L")
        var keys = arrayListOf("ART  ", "A08            ")
        for (i in 1..10){
            doChain(keys, dbFile)
        }
        keys = arrayListOf("ART  ", "MS01           ")
        for (i in 1..10){
            doChain(keys, dbFile)
        }
        keys = arrayListOf("ART  ", "SL03           ")
        for (i in 1..10){
            doChain(keys, dbFile)
        }
        keys = arrayListOf("ART  ", "TX01           ")
        for (i in 1..10){
            doChain(keys, dbFile)
        }
        keys = arrayListOf("ART  ", "MAN            ")
        for (i in 1..10){
            doChain(keys, dbFile)
        }
        dbManager!!.closeFile("BRARTI0L")
        dbManager!!.close()
    }

    private fun doChain(keys: List<String>, dbFile: DBFile){
        val chainResult = dbFile.chain(keys)
        assertEquals(keys[1], chainResult.record["A§ARTI"])
    }


}

