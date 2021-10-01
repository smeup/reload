package com.smeup.dbnative.nosql

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingLevel
import com.smeup.dbnative.metadata.file.PropertiesSerializer
import org.junit.Assert
import org.junit.Test

class NoSQLDBTestFilePerf {
    private lateinit var dbManager : NoSQLDBMManager

    fun initDBMManager() {
        dbManager = NoSQLDBMManager(
            ConnectionConfig("*",
                "mongodb://172.16.2.117:27017/smeup",
                "admin",
                "admin123",
                "com.mongodb.client.MongoClient")).apply {
            this.logger = Logger.getSimpleInstance(LoggingLevel.ALL)
            }
    }


    //02_CHAIN_5Keys1Time_BRARTI0F
    @Test
    fun testChain() {
        initDBMManager()
        dbManager.use {
            val fileMetadata =
                PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/", "BRARTI0L")
            dbManager!!.registerMetadata(fileMetadata, true)
            var dbFile = it!!.openFile("BRARTI0L")
            var keys = arrayListOf("ASACC0001      ")
            var result = dbFile.chain(keys)
            Assert.assertFalse(result.record.isEmpty())

            keys = arrayListOf("ASACC0002      ")
            result = dbFile.chain(keys)
            Assert.assertFalse(result.record.isEmpty())

            keys = arrayListOf("ASACC0003      ")
            result = dbFile.chain(keys)
            Assert.assertFalse(result.record.isEmpty())

            keys = arrayListOf("ASACC0004      ")
            result = dbFile.chain(keys)
            Assert.assertFalse(result.record.isEmpty())

            keys = arrayListOf("ASACC0005      ")
            result = dbFile.chain(keys)
            Assert.assertFalse(result.record.isEmpty())
        }
    }

    //05_CHAIN_NoFound_VERAPG0F
    @Test
    fun testChainNoFoundOpt() {
        initDBMManager()
        dbManager.use {
            val fileMetadata =
                PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/", "VERAPG3L")
            dbManager!!.registerMetadata(fileMetadata, true)
            var dbFile = it!!.openFile("VERAPG3L")
            var keys = arrayListOf("SMEGL.001", "20210111", "BONMAI")
            for (i in 1..10) {
                var result = dbFile.chain(keys)
                Assert.assertTrue(result.record.isEmpty())
            }
        }
    }

    //33_SetllAndReadE_DiffKey_100_VERAPG0F
    @Test
    fun testSetllReadeVERAPG0F(){
        initDBMManager()
        dbManager.use {
            val fileMetadata =
                PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/", "VERAPG3L")
            dbManager!!.registerMetadata(fileMetadata, true)
            var dbFile = it!!.openFile("VERAPG3L")
            var keys = arrayListOf("Q07/097        ", "AAAAA")
            dbFile.setll(keys)
            var result: Result
            for (i in 1..10) {
                result = dbFile.read()
            }
        }
    }

}