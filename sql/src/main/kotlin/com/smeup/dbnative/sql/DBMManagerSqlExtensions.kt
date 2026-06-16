package com.smeup.dbnative.sql

import com.smeup.dbnative.DBMManager
import javax.sql.DataSource

/**
 * Adapts this manager to a thread-scoped [DataSource] when it is SQL-based.
 */
fun DBMManager.toDataSource(): DataSource? =
    (this as? SQLDBMManager)?.let { ThreadScopedDataSource(it) }
