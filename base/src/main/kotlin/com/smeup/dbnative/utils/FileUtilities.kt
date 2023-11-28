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

package com.smeup.dbnative.utils

import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata

/*
    Return true if passed keys are all primary fields in metadata
 */
fun FileMetadata.matchFileKeys(keys: List<RecordField>): Boolean {
    val keysAsString = mutableListOf<String>()

    keys.forEach {
        keysAsString.add(it.name)
    }

    var result = true
    keysAsString.forEach {
        if (!fileKeys.contains(it)) {
            result = false
        }
    }

    return result
}

fun FileMetadata.getField(name: String): Field? {
    return if (fields.filter { it.name == name }.count() > 0) {
        fields.first { it.name == name }
    } else {
        null
    }
}

fun FileMetadata.fieldsToProperties(): MutableList<Pair<String, String>> {
    val properties = mutableListOf<Pair<String, String>>()

    for (field in fields.iterator()) {
        properties.add(
            Pair(
                "field.${field.name}",
                "${field.text}",
            ),
        )
    }
    return properties
}
