import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.log.LoggingEvent

enum class SQL_logging_key{
    setll, setgt, chain
}

class SQLLogger(defaultLoggingFunction: ((LoggingEvent<SQL_logging_key>) -> Unit)?): Logger<SQL_logging_key>(defaultLoggingFunction) {

    companion object{
        fun getSimpleInstance(): SQLLogger{
            return SQLLogger { println("${it.issueTime} - ${it.eventKey.name} - ${it.caller}: ${it.message}") }.apply {
                //addLoggingEvent(SQL_logging_key.setll).addLoggingEvent(SQL_logging_key.setgt)
                for(key in SQL_logging_key.values()){
                    addLoggingEvent(key)
                }
            }
        }
    }
}