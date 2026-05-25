package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.ConnectionProvider
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.log.Logger


/**
 * Configures [ConnectionProvider] using the manager module factory, bound to the `"default"` app key.
 *
 * @deprecated Use [configure] with an explicit app-keyed map instead.
 */
@Deprecated(
    message = "Use configure(configMap, logger) with an explicit app-keyed map instead.",
    replaceWith = ReplaceWith("configure(mapOf(\"default\" to config), logger)")
)
fun ConnectionProvider.configure(config: DBNativeAccessConfig, logger: Logger? = null) =
    configure(mapOf("default" to config), logger)

/**
 * Configures [ConnectionProvider] using the manager module factory.
 *
 * @param configMap One [DBNativeAccessConfig] per application key.
 * @param logger Optional logger forwarded to manager creation (overrides per-config logger).
 */
fun ConnectionProvider.configure(configMap: Map<String, DBNativeAccessConfig>, logger: Logger? = null) {
    configure(
        configMap,
        configMap.mapValues { (_, cfg) ->
            val effectiveLogger = logger ?: cfg.logger
            { connConfig: ConnectionConfig -> createDBManager(connConfig, effectiveLogger) }
        }
    )
}
