package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.log.Logger

/**
 * Configures [ConnectionProvider] using the manager module factory.
 *
 * @param config Native access configuration.
 * @param logger Optional logger forwarded to manager creation.
 */
fun ConnectionProvider.configure(config: DBNativeAccessConfig, logger: Logger? = null) {
    configure(config) { connConfig -> createDBManager(connConfig, logger) }
}
