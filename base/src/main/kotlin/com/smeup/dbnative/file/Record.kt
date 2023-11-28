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

package com.smeup.dbnative.file

class Record(vararg fields: RecordField) : LinkedHashMap<String, String>() {
    init {
        fields.forEach {
            add(it)
        }
    }

    fun matches(keyFields: List<RecordField>) =
        keyFields.all {
            val value1 = this[it.name]?.trim()
            val value2 = it.value.trim()

            value1.equals(value2.trim())
        }

    fun add(field: RecordField) {
        put(field.name, field.value)
    }

    fun duplicate(): Record {
        val thisMap = this
        return Record().apply { this.putAll(thisMap) }
    }
}
