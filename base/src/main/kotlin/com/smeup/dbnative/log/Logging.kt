package com.smeup.dbnative.log

import java.text.SimpleDateFormat
import java.util.*

enum class LoggingLevel{
    OFF, ERROR, WARN, INFO, DEBUG, TRACE, ALL
}

enum class LoggingKey(val level: LoggingLevel){
    native_access_method(LoggingLevel.TRACE),
    read_data(LoggingLevel.TRACE),
    execute_inquiry(LoggingLevel.DEBUG),
    search_data(LoggingLevel.DEBUG),
    connection(LoggingLevel.DEBUG)
}

data class LoggingEvent(val eventKey: LoggingKey, val message: String, val callerMethod: String? = null){
    val issueTime: Date = Date()
}

class Logger(val level:LoggingLevel = LoggingLevel.OFF, val loggingFunction: ((LoggingEvent) -> Unit)){
    companion object{
        fun getSimpleInstance(level:LoggingLevel = LoggingLevel.DEBUG): Logger{
            return Logger(level) { println("[${SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(it.issueTime)}][${it.eventKey.level}][${it.eventKey.name}][${it.callerMethod}] * ${it.message}") }
        }
    }
    fun logEvent(eventKey: LoggingKey, message: String): LoggingEvent?{
        return if(eventKey.level.ordinal <= level.ordinal) {
            val caller = Thread.currentThread().getStackTrace().getOrNull(2)?.let { "${it.className} ${it.methodName}:${it.lineNumber}" }?:""
            logEvent(LoggingEvent(eventKey, message, caller))
        }
        else{
            null
        }
    }

    internal fun logEvent(ev: LoggingEvent): LoggingEvent{
        loggingFunction.invoke(ev)
        return ev
    }
}

