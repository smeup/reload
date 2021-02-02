package com.smeup.dbnative.log

import java.lang.reflect.Method
import java.util.*


data class LoggingEvent<E: Enum<E>>(val eventKey: E, val message: String, val caller: String? = ""){
    val issueTime: Date = Date()
}

abstract class Logger<E: Enum<E>>(val defaultLoggingFunction: ((LoggingEvent<E>) -> Unit)? = null){
    private var loggingFunctions: MutableMap<E, (LoggingEvent<E>) -> Unit> = mutableMapOf()

    fun addLoggingEvent(loggingKey: E, loggingFunction: ((LoggingEvent<E>) -> Unit)? = null): Logger<E>{
        loggingFunction?.apply { }?:defaultLoggingFunction?.apply { loggingFunctions[loggingKey] = this }
        return this
    }

    fun addLoggingEvents(vararg eventKey: E){
        for(key in eventKey){
            defaultLoggingFunction?.apply { loggingFunctions[key] = this }
        }
    }

    fun logEvent(eventKey: E, message: String, caller: String?): LoggingEvent<E>{
        return logEvent(LoggingEvent(eventKey, message, caller))
    }

    internal fun logEvent(ev: LoggingEvent<E>): LoggingEvent<E>{
        loggingFunctions[ev.eventKey]?.invoke(ev).let { return ev }
    }
}

