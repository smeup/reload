package com.smeup.dbnative.sql

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.PoolConfig
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingEvent
import com.smeup.dbnative.log.LoggingKey
import com.smeup.dbnative.log.LoggingLevel
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

private val LOG_SQL_CONFIG = ConnectionConfig(
    fileName = "*",
    url = "jdbc:hsqldb:mem:LOG_NONPOOL_TEST",
    user = "sa",
    password = "root"
)

private val LOG_POOLED_CONFIG = ConnectionConfig(
    fileName = "*",
    url = "jdbc:hsqldb:mem:LOG_POOL_TEST",
    user = "sa",
    password = "root",
    poolConfig = PoolConfig(maximumPoolSize = 2)
)

private fun captureLogger(): Pair<Logger, MutableList<LoggingEvent>> {
    val events = mutableListOf<LoggingEvent>()
    return Logger(LoggingLevel.ALL) { events.add(it) } to events
}

/**
 * Verifies that connection lifecycle events are emitted to the logger
 * for both pooled and non-pooled managers.
 */
class ConnectionLoggingTest {

    @Before
    fun setUp() {
        ConnectionProvider.configure(DBNativeAccessConfig(listOf(LOG_SQL_CONFIG))) { SQLDBMManager(it) }
    }

    @After
    fun tearDown() {
        ConnectionProvider.configure(DBNativeAccessConfig(listOf(LOG_SQL_CONFIG))) { SQLDBMManager(it) }
    }

    @Test
    fun logger_propagatedToNonPooledManager() {
        val (logger, events) = captureLogger()
        val config = DBNativeAccessConfig(listOf(LOG_SQL_CONFIG), logger)
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                ConnectionProvider.requireDataSource("FILEA").getConnection()
            }
        } finally {
            handle.close()
        }
        assertTrue(events.any { it.eventKey == LoggingKey.connection },
            "Expected at least one connection event — logger was not propagated to the non-pooled manager")
    }

    @Test
    fun nonPooledManager_closeEventCarriesLifetime() {
        val (logger, events) = captureLogger()
        val config = DBNativeAccessConfig(listOf(LOG_SQL_CONFIG), logger)
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                ConnectionProvider.requireDataSource("FILEA").getConnection()
            }
        } finally {
            handle.close()
        }
        val closeEvent = events.find { it.eventKey == LoggingKey.connection && "Closing" in it.message }
        assertNotNull(closeEvent, "Expected a close connection event")
        assertNotNull(closeEvent.elapsedTime, "Close event must carry connection lifetime as elapsedTime")
    }

    @Test
    fun pooledManager_borrowEventCarriesTiming() {
        val (logger, events) = captureLogger()
        val config = DBNativeAccessConfig(listOf(LOG_POOLED_CONFIG), logger)
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                ConnectionProvider.requireDataSource("FILEA").getConnection()
            }
        } finally {
            handle.close()
        }
        val borrowEvent = events.find { it.eventKey == LoggingKey.connection && "Borrowed" in it.message }
        assertNotNull(borrowEvent, "Expected a borrow event from pooled manager")
        assertNotNull(borrowEvent.elapsedTime, "Borrow event must carry acquire time as elapsedTime")
    }

    @Test
    fun pooledManager_returnEventIsEmittedAfterScope() {
        val (logger, events) = captureLogger()
        val config = DBNativeAccessConfig(listOf(LOG_POOLED_CONFIG), logger)
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                ConnectionProvider.requireDataSource("FILEA").getConnection()
            }
        } finally {
            handle.close()
        }
        assertTrue(events.any { it.eventKey == LoggingKey.connection && "Returning" in it.message },
            "Expected a return event after the scope closes the pooled connection")
    }

    @Test
    fun pooledManager_noReturnEventWhenConnectionNeverAccessed() {
        val (logger, events) = captureLogger()
        val config = DBNativeAccessConfig(listOf(LOG_POOLED_CONFIG), logger)
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            ConnectionProvider.withScope {
                ConnectionProvider.currentManager("FILEA") // creates manager but does not borrow connection
            }
        } finally {
            handle.close()
        }
        assertFalse(events.any { it.eventKey == LoggingKey.connection && "Returning" in it.message },
            "Must not emit a return event when the pooled connection was never borrowed")
    }

    @Test
    fun configureWithPool_logsPoolCreationAndShutdown() {
        val (logger, events) = captureLogger()
        val config = DBNativeAccessConfig(listOf(LOG_POOLED_CONFIG), logger)
        val handle = ConnectionProvider.configureWithPool(config)
        try {
            assertTrue(events.any { it.eventKey == LoggingKey.connection && "Created" in it.message },
                "Expected pool creation event on configureWithPool")
        } finally {
            handle.close()
        }
        assertTrue(events.any { it.eventKey == LoggingKey.connection && "Shutting down" in it.message },
            "Expected pool shutdown event on handle close")
    }
}