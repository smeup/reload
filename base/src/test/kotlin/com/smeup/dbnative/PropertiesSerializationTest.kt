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

import com.smeup.dbnative.metadata.file.MetadataSerializer
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import org.junit.Test
import java.io.File

class DBFileFactoryTest {

    @Test
    fun loadAndSaveTest() {

        val fileMetadata = FileMetadata(
            "TEST",
            "ExampleTable",
            listOf(
                Field("field1", "some text", false),
                Field("field2", numeric = true)
            ),
            listOf("key1", "key2")
        )

        // Delete tmp file
        val tmpDir = System.getProperty("java.io.tmpdir")

        // Save metadata1 to tmp properties file
        MetadataSerializer.metadataToJson(tmpDir, fileMetadata, true)

        // Read metadata1 from properties
        var metadata = MetadataSerializer.jsonToMetadata(tmpDir, "TEST")

        // Compare metadatas class
        assert(metadata.name.equals("TEST"))
    }
}