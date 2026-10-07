package com.smeup.dbnative.sql

import com.smeup.dbnative.sql.utils.*
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import java.sql.SQLException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Engines like DB2 for i make a cursor read-only when its query is a UNION (SQL0510), so
 * [SQLDBFile] must delete/update through a searched statement by RRN instead of
 * `ResultSet.deleteRow()/updateRow()`. A dialect reporting the result set as not updatable
 * forces that path on any engine.
 */
class SQLReadOnlyCursorFallbackTest {
    companion object {
        private lateinit var dbManager: SQLDBMManager

        @BeforeClass
        @JvmStatic
        fun setUp() {
            dbManager = dbManagerForTest()
            createAndPopulateMunicipalityTable(dbManager)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dbManager.close()
            destroyDatabase()
        }
    }

    private val notUpdatableDialect = object : SQLDialect by DefaultSQLDialect() {
        override fun isResultSetUpdatable(sql: String) = false
    }

    private fun openWithNotUpdatableDialect() = SQLDBFile(
        MUNICIPALITY_TABLE_NAME,
        dbManager.metadataOf(MUNICIPALITY_TABLE_NAME),
        dbManager.connection,
        dialect = notUpdatableDialect
    )

    @Test
    fun deleteByRrnWhenResultSetNotUpdatable() {
        val dbFile = openWithNotUpdatableDialect()
        val key = buildMunicipalityKey("IT", "LOM", "BG", "CREDARO")
        dbFile.delete(dbFile.chain(key).record)
        dbFile.chain(key)
        assertTrue(dbFile.eof())
        dbFile.close()
    }

    @Test
    fun updateByRrnWhenResultSetNotUpdatable() {
        val dbFile = openWithNotUpdatableDialect()
        val key = buildMunicipalityKey("IT", "LOM", "BG", "ALBINO")
        val record = dbFile.chain(key).record
        record["PREF"] = "7777"
        dbFile.update(record)
        assertEquals("7777", dbFile.chain(key).record["PREF"]?.trim())
        dbFile.close()
    }

    @Test
    fun db2400DialectRecognisesReadOnlyCursorErrors() {
        val dialect = DB2400Dialect()
        assertTrue(dialect.isReadOnlyCursorError(SQLException("[SQL0510] read only", "42828")))
        assertTrue(dialect.isReadOnlyCursorError(SQLException("[SQL0906] previous error", "58003")))
        assertFalse(dialect.isReadOnlyCursorError(SQLException("[SQL0204] not found", "42704")))
        assertFalse(dialect.isResultSetUpdatable("SELECT A FROM T WHERE X = ? UNION SELECT A FROM T"))
        assertTrue(dialect.isResultSetUpdatable("SELECT A FROM T WHERE X = ?"))
        assertTrue(DefaultSQLDialect().isResultSetUpdatable("SELECT A FROM T UNION SELECT A FROM T"))
        assertFalse(DefaultSQLDialect().isReadOnlyCursorError(SQLException("[SQL0510]", "42828")))
    }
}
