package com.smeup.dbnative.sql

sealed class SqlDialect {
    open val defaultFetchSize: Int = 0

    object Default : SqlDialect()

    class PostgreSql(override val defaultFetchSize: Int = 1000) : SqlDialect()

    companion object {
        fun fromUrl(url: String, properties: Map<String, String> = emptyMap()): SqlDialect =
            if (url.contains("postgresql") || url.contains("postgres")) {
                val fetchSize = properties["fetchSize"]?.toInt() ?: 1000
                PostgreSql(fetchSize)
            } else Default
    }
}
