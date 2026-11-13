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


package com.smeup.dbnative.manager

import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.DBNativeAccessConfig
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

/**
 * A file opened with its metadata uses exactly that metadata and registers nothing, so opens of the
 * same file name with different metadata (e.g. one per tenant) do not interfere.
 */
class DBFileFactoryOpenWithMetadataTest {

    private val config = DBNativeAccessConfig(
        listOf(ConnectionConfig(fileName = "*", url = "class:com.smeup.dbnative.mock.MockDBManager", user = "*", password = "*"))
    )

    private fun metadata(tableName: String, field: String) =
        FileMetadata("OPENMD1L", tableName, listOf(Field(field)), listOf(field))

    @Test
    fun openWithMetadataDoesNotRegisterIt() {
        DBFileFactory(config).use { it.open("OPENMD1L", metadata("TENANT_A", "A1")) }
        assertFalse(DBManagerBaseImpl.register.contains("OPENMD1L"))
    }

    @Test
    fun eachOpenKeepsItsOwnMetadata() {
        DBFileFactory(config).use { factoryA ->
            DBFileFactory(config).use { factoryB ->
                val fileA = factoryA.open("OPENMD1L", metadata("TENANT_A", "A1"))
                val fileB = factoryB.open("OPENMD1L", metadata("TENANT_B", "B1"))

                assertEquals(metadata("TENANT_A", "A1"), fileA.fileMetadata)
                assertEquals(metadata("TENANT_B", "B1"), fileB.fileMetadata)
            }
        }
    }

    @Test
    fun theOpenedFileDoesNotShareTheCallersInstance() {
        val callerMetadata = metadata("TENANT_A", "A1")
        DBFileFactory(config).use { factory ->
            val file = factory.open("OPENMD1L", callerMetadata)
            callerMetadata.tableName = "CHANGED"
            assertEquals("TENANT_A", file.fileMetadata.tableName)
        }
    }

    @Test
    fun emptyTableNameDefaultsToTheFileName() {
        DBFileFactory(config).use { factory ->
            assertEquals("OPENMD1L", factory.open("OpenMd1L", metadata("", "A1")).fileMetadata.tableName)
        }
    }
}
