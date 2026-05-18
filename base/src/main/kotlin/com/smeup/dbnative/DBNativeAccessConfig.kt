/*
 * Copyright 2020 The Reload project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.smeup.dbnative

import com.smeup.dbnative.log.Logger

/**
 * Configuration for DB native access.
 *
 * @param connectionsConfig List of available connection configurations.
 * @param logger Optional logger implementation.
 */
data class DBNativeAccessConfig (val connectionsConfig: List<ConnectionConfig>, val logger: Logger? = null){
    constructor(connectionsConfig: List<ConnectionConfig>):this(connectionsConfig, null)
}

/**
 * Creates a connection configuration for a single file or a file group.
 *
 * @param fileName File or file-group identifier. The `*` wildcard is supported
 * (for example, `*tablename` matches files ending with `tablename`).
 * @param url Connection URL. The protocol can be customized.
 * @param user Username.
 * @param password Password.
 * @param driver JDBC driver class name, when required.
 * @param impl DB manager implementation. If not specified, it is inferred from `url`.
 * @param properties Additional connection properties.
 * @param poolConfig Connection pool configuration.
 * */
data class ConnectionConfig @JvmOverloads constructor(
    val fileName: String,
    val url: String,
    val user: String,
    val password: String,
    val driver: String? = null,
    val impl: String? = null,
    val properties: Map<String, String> = mutableMapOf(),
    val poolConfig: PoolConfig? = null
)

