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

data class DBNativeAccessConfig (val connectionsConfig: List<ConnectionConfig>)


/**
 * Create a new instance of connection configuratio for a single file or file groups.
 * @param fileName File or file group identifier, wildcard "*" is admitted.
 * I.E. file=*tablename is for all files starts with <code>tablename</code>
 * @param url Connection url, protocol could be customized
 * @param user The user
 * @param password The password
 * @param driver If needed
 * @param impl DBMManager implementation. If doesn't specified is assumed by url
 * @param properties Others connection properties
 * */
data class ConnectionConfig (
    val fileName: String,
    val url: String,
    val user: String,
    val password: String,
    val driver: String? = null,
    val impl: String? = null,
    val properties : Map<String, String> = mutableMapOf())

