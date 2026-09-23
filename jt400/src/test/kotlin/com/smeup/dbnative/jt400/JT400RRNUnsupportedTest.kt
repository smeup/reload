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

package com.smeup.dbnative.jt400

import com.ibm.as400.access.KeyedFile
import com.smeup.dbnative.model.FileMetadata
import org.junit.Test
import kotlin.test.assertFailsWith

/**
 * The JT400 backend only implements keyed access (it's built entirely around JTOpen's
 * `KeyedFile` API). These checks need no live AS400 connection: [JT400DBFile.requireKeyed] throws
 * before `file` is ever touched, so a plain no-arg [KeyedFile] (never connected/opened) is enough.
 */
class JT400RRNUnsupportedTest {

    private fun unkeyedDBFile() =
        JT400DBFile("TEST", FileMetadata("TEST", "TEST", emptyList(), emptyList()), KeyedFile())

    @Test
    fun chainOnUnkeyedFileThrowsUnsupportedOperationException() {
        assertFailsWith<UnsupportedOperationException> { unkeyedDBFile().chain(listOf("1")) }
    }

    @Test
    fun setllOnUnkeyedFileThrowsUnsupportedOperationException() {
        assertFailsWith<UnsupportedOperationException> { unkeyedDBFile().setll(listOf("1")) }
    }

    @Test
    fun setgtOnUnkeyedFileThrowsUnsupportedOperationException() {
        assertFailsWith<UnsupportedOperationException> { unkeyedDBFile().setgt(listOf("1")) }
    }

    @Test
    fun readEqualOnUnkeyedFileThrowsUnsupportedOperationException() {
        assertFailsWith<UnsupportedOperationException> { unkeyedDBFile().readEqual(listOf("1")) }
    }

    @Test
    fun readPreviousEqualOnUnkeyedFileThrowsUnsupportedOperationException() {
        assertFailsWith<UnsupportedOperationException> { unkeyedDBFile().readPreviousEqual(listOf("1")) }
    }
}
