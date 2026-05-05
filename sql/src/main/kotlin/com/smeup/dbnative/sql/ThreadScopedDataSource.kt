package com.smeup.dbnative.sql

import java.io.PrintWriter
import java.sql.Connection
import java.util.logging.Logger
import javax.sql.DataSource

/**
 * [DataSource] facade that always returns the current scoped connection of [manager].
 */
class ThreadScopedDataSource(private val manager: SQLDBMManager) : DataSource {

    // manager.connection is a lazy val → same Connection instance for the entire withScope() scope
    override fun getConnection(): Connection = manager.connection
    override fun getConnection(username: String, password: String): Connection = manager.connection

    override fun getLogWriter(): PrintWriter? = null
    override fun setLogWriter(out: PrintWriter?) {}
    override fun getLoginTimeout(): Int = 0
    override fun setLoginTimeout(seconds: Int) {}
    override fun getParentLogger(): Logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)
    override fun isWrapperFor(iface: Class<*>): Boolean = iface.isInstance(this)
    override fun <T> unwrap(iface: Class<T>): T = iface.cast(this)
}
