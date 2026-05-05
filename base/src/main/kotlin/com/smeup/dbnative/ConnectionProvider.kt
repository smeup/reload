package com.smeup.dbnative

/**
 * Provides thread-scoped access to [DBMManager] instances.
 */
object ConnectionProvider {

    private val threadLocal = ThreadLocal<MutableMap<ConnectionConfig, DBMManager>>()

    @Volatile private var config: DBNativeAccessConfig? = null
    @Volatile private var managerFactory: ((ConnectionConfig) -> DBMManager)? = null

    /**
     * Configures the provider with connection matching rules and a manager factory.
     */
    fun configure(config: DBNativeAccessConfig, factory: (ConnectionConfig) -> DBMManager) {
        this.config = config
        this.managerFactory = factory
    }

    /**
     * Functional interface used by [withScope] to execute a scoped block.
     */
    fun interface ScopedBlock {
        @Throws(Exception::class)
        fun execute()
    }

    /**
     * Runs [block] in a thread-local scope and closes all managers created in that scope.
     */
    @Throws(Exception::class)
    fun withScope(block: ScopedBlock) {
        requireNotNull(config) { "ConnectionProvider not configured" }
        threadLocal.set(mutableMapOf())
        try {
            block.execute()
        } finally {
            val managers = threadLocal.get()
            threadLocal.remove()
            managers?.values?.forEach { it.close() }
        }
    }

    /**
     * Returns the current scope manager for [fileName], creating it on first use.
     */
    fun currentManager(fileName: String): DBMManager {
        val managers = requireNotNull(threadLocal.get()) { "No active scope on this thread" }
        val cfg = requireNotNull(config)
        val factory = requireNotNull(managerFactory)
        val connectionConfig = findConnectionConfigFor(fileName, cfg.connectionsConfig)
        return managers.getOrPut(connectionConfig) { factory(connectionConfig) }
    }

    /**
     * Like [currentManager], but returns `null` if there is no active scope or no match.
     */
    fun currentManagerOrNull(fileName: String): DBMManager? {
        val managers = threadLocal.get() ?: return null
        val cfg = config ?: return null
        val factory = managerFactory ?: return null
        return try {
            val connectionConfig = findConnectionConfigFor(fileName, cfg.connectionsConfig)
            managers.getOrPut(connectionConfig) { factory(connectionConfig) }
        } catch (e: IllegalArgumentException) {
            null
        }
    }
}
