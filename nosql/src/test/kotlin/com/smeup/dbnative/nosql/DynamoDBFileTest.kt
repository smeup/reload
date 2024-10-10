package com.smeup.dbnative.nosql

import com.amazonaws.services.dynamodbv2.model.*
import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingLevel
import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.model.VarcharType
import com.smeup.dbnative.nosql.utils.MUNICIPALITY_TABLE_NAME
import com.smeup.dbnative.utils.TypedMetadata
import com.smeup.dbnative.utils.fieldByType
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import java.io.File
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

//@Ignore
class DynamoDBFileTest {

    val tableName = MUNICIPALITY_TABLE_NAME


    companion object {
        private lateinit var dbManager: NoSQLDBMManager
        private var connectionConfig = ConnectionConfig(
            "*",
            "http://localhost:8000",
            "",
            "",
            properties = mutableMapOf(
                "REGION" to "eu-west-1",
                "AWS_ACCESS_KEY_ID" to "pc9u7",
                "AWS_SECRET_ACCESS_KEY" to "g08sh"
            )
        )

        @BeforeClass
        @JvmStatic
        fun initEnv() {
            dbManager = NoSQLDBMManager(connectionConfig)
            val fields = listOf(
                "£NAZ" fieldByType CharacterType(2),
                "§REG" fieldByType CharacterType(3),
                "PROV" fieldByType CharacterType(2),
                "CITTA" fieldByType VarcharType(35),
                "CAP" fieldByType CharacterType(5),
                "PREF" fieldByType CharacterType(4),
                "COMUNE" fieldByType CharacterType(4),
                "ISTAT" fieldByType CharacterType(6)
            )

            val keys = listOf(
                "£NAZ",
                "§REG",
                "PROV",
                "CITTA"
            )
            val tMetadata = TypedMetadata(MUNICIPALITY_TABLE_NAME, MUNICIPALITY_TABLE_NAME, fields, keys)
            dbManager.registerMetadata(tMetadata.fileMetadata(), true)
            dbManager.logger = (Logger.getSimpleInstance(LoggingLevel.TRACE))


            createDynamoDBTable(MUNICIPALITY_TABLE_NAME)

            val csvPath = "src/test/resources/csv/Municipality.csv"
            populateTableFromCSV(csvPath)


        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dbManager.dynamoDBAsyncClient.deleteTable(MUNICIPALITY_TABLE_NAME)
            println("Table $MUNICIPALITY_TABLE_NAME dropped.")
            dbManager.close()
        }

        private fun createDynamoDBTable(tableName: String) {
            // Create a DynamoDB client
            val ddb = dbManager.dynamoDBAsyncClient

            try {
                // Define the table schema and attributes
                val createTableRequest = CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(
                        KeySchemaElement("ISTAT", "HASH")
                    )
                    .withAttributeDefinitions(
//                        AttributeDefinition("£NAZ", "S"),
//                        AttributeDefinition("§REG" , "S"),
//                        AttributeDefinition("PROV" , "S"),
//                        AttributeDefinition("CITTA" , "S"),
//                        AttributeDefinition("CAP" , "S"),
//                        AttributeDefinition("PREF" , "S"),
//                        AttributeDefinition("COMUNE" , "S"),
                        AttributeDefinition("ISTAT", "S"),

                        ).withProvisionedThroughput(
                        ProvisionedThroughput().withReadCapacityUnits(50).withWriteCapacityUnits(50)
                    )

                // Create the table
                val createTableResponse: CreateTableResult = ddb.createTable(createTableRequest)
                println("Table created successfully: ${createTableResponse.tableDescription.tableName}")

            } catch (e: Exception) {
                println("Failed to create table: ${e.message}")
            }
        }

        private fun populateTableFromCSV(csvPath: String) {
            val dbFile: DynamoDBFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME) as DynamoDBFile
            val file = File(csvPath)

            val dataRows: List<Map<String, String>> = csvReader().readAllWithHeader(file)

            dataRows.forEach {
                val recordFields = emptyList<RecordField>().toMutableList()
                it.map { (key, value) ->
                    recordFields.add(RecordField(key, value))
                }
                dbFile.write(Record(*recordFields.toTypedArray()))
            }

            dbFile.close()
            println("Table population completed.")
        }
    }

    @Test
    fun testConnection() {
        try {
            dbManager.validateConfig()
        } catch (exc: Exception) {
            Assert.fail("Expected no exception, but got: ${exc.message}")
        }
    }

    @Test
    fun testsetLL() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile

        assertTrue(dbFile.setll(mutableListOf("IT")))
        assertTrue(dbFile.setll(mutableListOf("IT", "ABR")))
        assertTrue(dbFile.setll(mutableListOf("IT", "ABR", "AQ")))
        assertTrue(dbFile.setll(mutableListOf("IT", "ABR", "AQ", "CASTEL DEL MONTE")))

        dbFile.close()

    }

    @Test
    fun testsetGT() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile

        assertTrue(dbFile.setgt(mutableListOf("IT")))
        assertTrue(dbFile.setgt(mutableListOf("IT", "ABR")))
        assertTrue(dbFile.setgt(mutableListOf("IT", "ABR", "AQ")))
        assertTrue(dbFile.setgt(mutableListOf("IT", "ABR", "AQ", "CASTEL DEL MONTE")))

        dbFile.close()

    }

    @Test
    fun testCHAIN() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile

        val chainResult = (dbFile.chain(mutableListOf("IT", "ABR")))
        val chainResult1 = (dbFile.chain(mutableListOf("IT", "ABR", "CH")))

        assertEquals("CASTEL DEL MONTE", chainResult.record["CITTA"])
        assertEquals("PRETORO", chainResult1.record["CITTA"])
        dbFile.close()

    }

    @Test
    fun testmultipleREAD() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile
        val keys = mutableListOf("IT", "ABR", "AQ")
        assertTrue(dbFile.setll(keys = keys))

        assertEquals("CASTEL DEL MONTE", dbFile.read().record["CITTA"])
        assertEquals("SANTO STEFANO DI SESSANIO", dbFile.read().record["CITTA"])
        assertEquals("LECCE NEI MARSI", dbFile.read().record["CITTA"])
        assertEquals("SAN VINCENZO VALLE ROVETO", dbFile.read().record["CITTA"])
        assertEquals("AIELLI", dbFile.read().record["CITTA"])
        assertEquals("ACCIANO", dbFile.read().record["CITTA"])

        dbFile.close()

    }

    @Test
    fun testmultipleREADEQUAL() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile
        val keys = mutableListOf("IT", "ABR", "AQ")
        assertTrue(dbFile.setll(keys = keys))

        assertEquals("CASTEL DEL MONTE", dbFile.readEqual(keys).record["CITTA"])
        assertEquals("SANTO STEFANO DI SESSANIO", dbFile.readEqual(keys).record["CITTA"])
        assertEquals("LECCE NEI MARSI", dbFile.readEqual(keys).record["CITTA"])
        assertEquals("SAN VINCENZO VALLE ROVETO", dbFile.readEqual(keys).record["CITTA"])
        assertEquals("AIELLI", dbFile.readEqual(keys).record["CITTA"])
        assertEquals("ACCIANO", dbFile.readEqual(keys).record["CITTA"])

        dbFile.close()

    }

    @Test
    fun testWRITE() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile

        val recordToEdit = dbFile.chain(mutableListOf("IT", "ABR", "CH"))

        recordToEdit.record["PROV"] = "BA"
        recordToEdit.record["ISTAT"] = "069999"

        val writtenRecord = dbFile.write(recordToEdit.record)

        if (writtenRecord.errorMsg.isEmpty()) {
            assertEquals("BA", writtenRecord.record["PROV"])
        } else {
            Assert.fail("Expected no error, but got: ${(writtenRecord.errorMsg)}")
        }

        dbFile.close()

    }

    @Test
    fun testUPDATE() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile

        val recordToEdit = dbFile.chain(mutableListOf("IT", "ABR", "CH"))


        recordToEdit.record["PROV"] = "BA"

        val updatedRecord = dbFile.update(recordToEdit.record)

        if (updatedRecord.errorMsg.isEmpty()) {
            assertEquals("BA", updatedRecord.record["PROV"])
        } else {
            Assert.fail("Expected no error, but got: ${(updatedRecord.errorMsg)}")
        }

        dbFile.close()

    }

    @Test
    fun testDELETE() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile

        val recordToDelete = dbFile.chain(mutableListOf("IT", "ABR", "CH"))

        val deletedRecord = dbFile.delete(recordToDelete.record)

        if (deletedRecord.errorMsg.isNotEmpty()) {
            Assert.fail("Expected no error, but got: ${(deletedRecord.errorMsg)}")
        }

        dbFile.close()

    }

    @Test
    fun testDESCRIBE() {
        val dbFile: DynamoDBFile = dbManager.openFile(tableName) as DynamoDBFile

        println(dbFile.getPrimaryKey(tableName))

        dbFile.close()

    }


}