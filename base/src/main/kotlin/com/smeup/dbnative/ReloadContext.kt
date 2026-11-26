package com.smeup.dbnative

/**
 * Information bound to a [ConnectionProvider] scope, available to every reload component running
 * inside it (see [ConnectionProvider.withScope] and [ConnectionProvider.currentContextOrNull]).
 *
 * New information is added as a new property of this class, with a default value, so that the
 * signature of [ConnectionProvider.withScope] never has to change again.
 *
 * @param sessionId identifier of the user session the scope is running for. It is used to filter,
 * and to stamp, the rows of session tables (tables having the `__SID` column, see
 * `docs/development.md`). `null` when the scope is not bound to a session.
 */
data class ReloadContext(
    val sessionId: String? = null
) {
    companion object {
        /** A context carrying no information. */
        @JvmField
        val EMPTY = ReloadContext()
    }
}
