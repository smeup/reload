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
import com.smeup.dbnative.metadata.MetadataRegister
import com.smeup.dbnative.metadata.file.FSMetadataRegisterImpl
import com.smeup.dbnative.model.FileMetadata
import java.util.*

abstract class DBManagerBaseImpl : DBMManager {
    var logger: Logger? = null
   /*
    val metadataRegister: MetadataRegister
        get() {
            return FSMetadataRegisterImpl
        }

    */

    override fun metadataOf(name: String): FileMetadata {
        return getMetadataRegister().getMetadata(name.uppercase(Locale.getDefault()))
    }

    override fun registerMetadata(
        metadata: FileMetadata,
        overwrite: Boolean,
    ) {
        if (getMetadataRegister().contains(metadata.name)) {
            if (overwrite) {
                getMetadataRegister().remove(metadata.name)
            } else {
                return
            }
            // TODO: send exception (existent metadata and no overwrite)
        }

        getMetadataRegister().registerMetadata(metadata, overwrite)
    }

    override fun unregisterMetadata(name: String) {
        if (getMetadataRegister().contains(name)) {
            getMetadataRegister().remove(name)
        }
    }

    override fun existFile(name: String): Boolean {
        return getMetadataRegister().contains(name.uppercase())
    }

    companion object {
        val register: MetadataRegister
            get() {
                return FSMetadataRegisterImpl
            }

        fun getMetadataRegister(): MetadataRegister {
            return register
        }

        fun staticRegisterMetadata(
            metadata: FileMetadata,
            overwrite: Boolean,
        ) {
            if (getMetadataRegister().contains(metadata.name)) {
                if (overwrite) {
                    getMetadataRegister().remove(metadata.name)
                } else {
                    return
                }
                // TODO: send exception (existent metadata and no overwrite)
            }

            getMetadataRegister().registerMetadata(metadata, overwrite)
        }

        fun staticUnregisterMetadata(name: String) {
            if (getMetadataRegister().contains(name)) {
                getMetadataRegister().remove(name)
            }
        }

        fun staticGetMetadata(name: String): FileMetadata {
            return getMetadataRegister().getMetadata(name)
        }
    }
}
