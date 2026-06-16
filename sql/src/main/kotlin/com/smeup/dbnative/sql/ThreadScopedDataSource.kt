package com.smeup.dbnative.sql

import java.io.PrintWriter
import java.sql.Connection
import java.util.logging.Logger
import javax.sql.DataSource

/**
 * [DataSource] facade that always returns the current scoped connection of [manager].
 *
 * [getConnection] returns a [NonCloseableConnection] wrapper so that callers following the
 * standard JDBC idiom of closing every acquired connection cannot accidentally close the shared
 * scope connection. The real connection is closed only when the scope ends via
 * [SQLPooledDBMManager.close].
 */
class ThreadScopedDataSource(private val manager: SQLDBMManager) : DataSource {

    private val cachedWrapper by lazy { NonCloseableConnection(manager.connection) }

    override fun getConnection(): Connection = cachedWrapper
    override fun getConnection(username: String, password: String): Connection = cachedWrapper

    override fun getLogWriter(): PrintWriter? = null
    override fun setLogWriter(out: PrintWriter?) {}
    override fun getLoginTimeout(): Int = 0
    override fun setLoginTimeout(seconds: Int) {}
    override fun getParentLogger(): Logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)
    override fun isWrapperFor(iface: Class<*>): Boolean = iface.isInstance(this)
    override fun <T> unwrap(iface: Class<T>): T = iface.cast(this)

    private class NonCloseableConnection(delegate: Connection) : Connection by delegate {
        override fun close() {}
        override fun isClosed(): Boolean = false
    }
}
