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


package com.smeup.dbnative.metadata

import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotSame
import kotlin.test.assertTrue

class InMemoryMetadataRegisterTest {

    private fun metadata(tableName: String, field: String = "F1") =
        FileMetadata("Test1L", tableName, listOf(Field(field)), listOf(field))

    @Test
    fun registeredMetadataIsFoundIgnoringCase() {
        val register = InMemoryMetadataRegister()
        register.registerMetadata(metadata("TEST1F"), true)

        assertTrue(register.contains("TEST1L"))
        assertTrue(register.contains("test1l"))
        assertEquals(metadata("TEST1F"), register.getMetadata("TEST1L"))
    }

    @Test
    fun overwriteReplacesAndNoOverwriteKeeps() {
        val register = InMemoryMetadataRegister()
        register.registerMetadata(metadata("A"), true)
        register.registerMetadata(metadata("B"), false)
        assertEquals("A", register.getMetadata("TEST1L").tableName)

        register.registerMetadata(metadata("B"), true)
        assertEquals("B", register.getMetadata("TEST1L").tableName)
    }

    @Test
    fun callersNeverShareTheRegisteredInstance() {
        val register = InMemoryMetadataRegister()
        val registered = metadata("TEST1F")
        register.registerMetadata(registered, true)
        registered.tableName = "CHANGED"

        val read = register.getMetadata("TEST1L")
        assertEquals("TEST1F", read.tableName)
        read.tableName = "CHANGED"
        assertNotSame(read, register.getMetadata("TEST1L"))
        assertEquals("TEST1F", register.getMetadata("TEST1L").tableName)
    }

    @Test
    fun emptyTableNameDefaultsToTheFileName() {
        val register = InMemoryMetadataRegister()
        register.registerMetadata(metadata(""), true)
        assertEquals("TEST1L", register.getMetadata("test1l").tableName)
    }

    @Test
    fun removedOrMissingMetadataIsNotFound() {
        val register = InMemoryMetadataRegister()
        register.registerMetadata(metadata("TEST1F"), true)
        register.remove("test1l")

        assertFalse(register.contains("TEST1L"))
        assertFailsWith<NoSuchElementException> { register.getMetadata("TEST1L") }
    }
}
