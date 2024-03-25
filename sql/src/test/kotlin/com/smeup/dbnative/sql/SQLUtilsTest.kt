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

package com.smeup.dbnative.sql

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.model.CharacterType
import com.smeup.dbnative.model.DecimalType
import com.smeup.dbnative.sql.utils.toSQL
import org.junit.Test
import kotlin.test.assertEquals

class SQLUtilsTest {
    @Test
    fun sqlForInsertTest() {
        val record = Record(
            RecordField("TSTFLDCHR", "XXX"),
            RecordField("TSTFLDNBR", "123.45")
        )
        assertEquals("INSERT INTO TSTTAB (\"TSTFLDCHR\", \"TSTFLDNBR\") VALUES(?, ?)", "TSTTAB".insertSQL(record))
    }

    @Test
    fun sqlForOrderBy() {
        val fields = listOf("Field1", "Field2", "Field3")
        assertEquals("ORDER BY \"Field1\", \"Field2\", \"Field3\"", orderBySQL(fields))
        assertEquals("ORDER BY \"Field1\" DESC, \"Field2\" DESC, \"Field3\" DESC", orderBySQL(fields, true))
    }
}
