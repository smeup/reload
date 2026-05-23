package com.smeup.dbnative

/**
 * Provides thread-scoped access to [DBMManager] instances, keyed by application.
 *
 * Call [configure] once at startup with one [DBNativeAccessConfig] per app key.
 * Wrap each FUN dispatch with [withScope] to bind an app to the current thread.
 * Code deeper in the stack calls [currentManager] or [currentManagerOrNull] without
 * knowing which app is active.
 */
object ConnectionProvider {

    private val threadLocal = ThreadLocal<Pair<String, MutableMap<ConnectionConfig, DBMManager>>>()

    @Volatile private var configMap: Map<String, DBNativeAccessConfig>? = null
    @Volatile private var factoryMap: Map<String, (ConnectionConfig) -> DBMManager>? = null

    /**
     * Configures the provider with one config and factory per application key.
     */
    fun configure(
        configMap: Map<String, DBNativeAccessConfig>,
        factoryMap: Map<String, (ConnectionConfig) -> DBMManager>
    ) {
        this.configMap = configMap
        this.factoryMap = factoryMap
    }

    /**
     * Returns `true` if [configure] has been called with at least one entry.
     */
    fun isConfigured(): Boolean = configMap?.isNotEmpty() == true

    /**
     * Functional interface used by [withScope] to execute a scoped block.
     */
    fun interface ScopedBlock {
        @Throws(Exception::class)
        fun execute()
    }

    /**
     * Runs [block] in a thread-local scope bound to [app], then closes all managers
     * created during that scope.
     *
     * @throws IllegalStateException if [configure] has not been called.
     * @throws IllegalArgumentException if [app] has no entry in the config map.
     */
    @Throws(Exception::class)
    fun withScope(app: String, block: ScopedBlock) {
        requireNotNull(configMap) { "ConnectionProvider not configured" }
        threadLocal.set(app to mutableMapOf())
        try {
            block.execute()
        } finally {
            val (_, managers) = threadLocal.get()
            threadLocal.remove()
            managers.values.forEach { it.close() }
        }
    }

    /**
     * Returns the current scope manager for [fileName], creating it on first use.
     *
     * @throws IllegalStateException if there is no active scope on this thread.
     * @throws IllegalArgumentException if the active app has no config entry or [fileName]
     *   matches no [ConnectionConfig].
     */
    fun currentManager(fileName: String): DBMManager {
        val (app, managers) = requireNotNull(threadLocal.get()) { "No active scope on this thread" }
        val cfg = requireNotNull(configMap?.get(app)) { "No configuration for app '$app'" }
        val factory = requireNotNull(factoryMap?.get(app)) { "No factory for app '$app'" }
        val connectionConfig = findConnectionConfigFor(fileName, cfg.connectionsConfig)
        return managers.getOrPut(connectionConfig) { factory(connectionConfig) }
    }

    /**
     * Like [currentManager], but returns `null` if there is no active scope, no config for
     * the active app, or no matching [ConnectionConfig] for [fileName].
     */
    fun currentManagerOrNull(fileName: String): DBMManager? {
        val pair = threadLocal.get() ?: return null
        val (app, managers) = pair
        val cfg = configMap?.get(app) ?: return null
        val factory = factoryMap?.get(app) ?: return null
        return try {
            val connectionConfig = findConnectionConfigFor(fileName, cfg.connectionsConfig)
            managers.getOrPut(connectionConfig) { factory(connectionConfig) }
        } catch (e: IllegalArgumentException) {
            null
        }
    }
}
