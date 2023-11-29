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

import com.smeup.dbnative.model.FileMetadata

interface MetadataRegister {

    fun registerMetadata(metadata: FileMetadata, overwrite: Boolean)
    fun getMetadata(filename:String): FileMetadata
    fun contains(fileName:String): Boolean
    fun remove(fileName:String)
}