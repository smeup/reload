package com.smeup.dbnative.log

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LoggerIsEnabledTest {

    @Test
    fun offLevelEnablesNothing() {
        val logger = Logger(LoggingLevel.OFF) { }
        LoggingKey.values().forEach { assertFalse(logger.isEnabled(it), "$it must be disabled at OFF") }
    }

    @Test
    fun infoLevelSkipsTraceAndDebugKeys() {
        // The SDK falls back to INFO when the slf4j channel is OFF: TRACE/DEBUG keys must stay disabled.
        val logger = Logger(LoggingLevel.INFO) { }
        assertFalse(logger.isEnabled(LoggingKey.native_access_method))
        assertFalse(logger.isEnabled(LoggingKey.read_data))
        assertFalse(logger.isEnabled(LoggingKey.connection))
    }

    @Test
    fun traceAndAllLevelsEnableTheKeys() {
        listOf(LoggingLevel.TRACE, LoggingLevel.ALL).forEach { level ->
            val logger = Logger(level) { }
            LoggingKey.values().forEach { assertTrue(logger.isEnabled(it), "$it must be enabled at $level") }
        }
    }

    @Test
    fun isEnabledIsConsistentWithLogEvent() {
        // logEvent delivers an event if and only if isEnabled says so
        LoggingLevel.values().forEach { level ->
            val events = mutableListOf<LoggingEvent>()
            val logger = Logger(level) { events.add(it) }
            LoggingKey.values().forEach { key ->
                val before = events.size
                logger.logEvent(key, "m")
                assertEquals(logger.isEnabled(key), events.size == before + 1, "level=$level key=$key")
            }
        }
    }
}
