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

package com.smeup.reload

import com.smeup.reload.metadata.file.PropertiesSerializer
import org.junit.Test
import java.io.File

class DBFileFactoryTest {

    @Test
    fun loadAndSaveTest() {
        // Delete tmp file
        var tmpFile = File("src/test/resources/dds/properties/out/BRARTI0F.properties")
        if (tmpFile.exists()) tmpFile.delete()

        // Read metadata1 from properties
        var metadata1 = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/", "BRARTI0F")
        println(metadata1)

        // Save metadata1 to tmp properties file
        PropertiesSerializer.metadataToProperties("src/test/resources/dds/properties/out", metadata1, true)

        // Read metadata2 from tmp properties file
        var metadata2 = PropertiesSerializer.propertiesToMetadata("src/test/resources/dds/properties/out/", "BRARTI0F")

        // Compare metadatas class
        assert(metadata2.equals(metadata1))
    }
}