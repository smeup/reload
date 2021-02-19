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

enum class NativeMethod(){
    equal, setll, setgt, chain, read, readPrevious, readEqual, readPreviousEqual, write, update, delete;
}

data class LoggingEvent(val eventKey: LoggingKey,
                        val message: String,
                        val callerMethod: String? = null,
                        val elapsedTime: Long? = null,
                        val nativeMethodCall: NativeMethod? = null,
                        val fileName: String? = null){
    val issueTime: Date = Date()
    fun isMeasuredEvent() = elapsedTime != null
}

open class Logger(val level:LoggingLevel = LoggingLevel.OFF,
             var loggingFunction: ((LoggingEvent) -> Unit)?)
{
    companion object{
        @JvmStatic
        fun getSimpleInstance(level:LoggingLevel = LoggingLevel.DEBUG): Logger{
            return Logger(level, loggingFunction = {
                val logged = "[%s][%s][%s][%s][%s] * %s %s"
                println(logged.format(SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(it.issueTime),
                    it.eventKey.level,
                    it.eventKey.name,
                    it.nativeMethodCall?:"",
                    it.fileName?:"",
                    it.message,
                    if(it.isMeasuredEvent()) "(${it.elapsedTime} ms)" else "")
                    )
            })
        }
    }

    fun logEvent(eventKey: LoggingKey,
                 message: String,
                 elapsedTime: Long? = null,
                 nativeMethodCall: NativeMethod? = null,
                 fileName: String? = null): LoggingEvent?{
        return if(eventKey.level.ordinal <= level.ordinal) {
            val caller = Thread.currentThread().getStackTrace().getOrNull(2)?.let {
                "${it.className} ${it.methodName}:${it.lineNumber}"
            }?:""
            logEvent(LoggingEvent(eventKey, message, caller, elapsedTime, nativeMethodCall, fileName))
        }
        else{
            null
        }
    }

    internal fun logEvent(ev: LoggingEvent): LoggingEvent{
        loggingFunction?.invoke(ev)
        return ev
    }
}

