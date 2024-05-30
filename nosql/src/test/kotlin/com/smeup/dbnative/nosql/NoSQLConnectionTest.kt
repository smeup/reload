/*
 *  Copyright 2024 The Reload project Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.smeup.dbnative.nosql

import com.smeup.dbnative.ConnectionConfig
import org.junit.Assert
import kotlin.test.Test
import kotlin.test.assertFailsWith

class NoSQLConnectionTest {

    @Test
    fun testUrl() {

        val testConnectionStrings = listOf(
            "mongodb://username:password@host:27017/database",
            "mongodb://host:27017/database",
            "mongodb://invalid:connection@string"
        )

        var noSQLDBMManager = NoSQLDBMManager(ConnectionConfig("*", testConnectionStrings[0], "", ""))
        try {
            noSQLDBMManager.validateConfig()
        } catch (exc: Exception) {
            Assert.fail("Expected no exception, but got: ${exc.message}")
        }

        noSQLDBMManager = NoSQLDBMManager(ConnectionConfig("*", testConnectionStrings[1], "", ""))
        try {
            noSQLDBMManager.validateConfig()
        } catch (exc: Exception) {
            Assert.fail("Expected no exception, but got: ${exc.message}")
        }

        noSQLDBMManager = NoSQLDBMManager(ConnectionConfig("*", testConnectionStrings[2], "", ""))
        assertFailsWith<RuntimeException>("Expected exception, but none was thrown") {
            noSQLDBMManager.validateConfig()
        }
    }
}