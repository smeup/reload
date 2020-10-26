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

package com.smeup.reload.model

enum class Type {
    CHARACTER,
    VARCHAR,
    INTEGER,
    SMALLINT,
    BIGINT,
    BOOLEAN,
    DECIMAL,
    FLOAT,
    DOUBLE,
    TIMESTAMP,
    TIME,
    DATE,
    BINARY,
    VARBINARY
}

sealed class FieldType {

        abstract val type: Type
        abstract val size: Int
        abstract val digits: Int
        abstract val fixedSize: Boolean

    }

// Fixed length string
    data class CharacterType(val length: Int) : FieldType() {

        override val type: Type
            get() = Type.CHARACTER

        override val size: Int
            get() = length

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = false
    }

// Varying length string (with max length)
    data class VarcharType(val length: Int) : FieldType() {

        override val type: Type
            get() = Type.VARCHAR

        override val size: Int
            get() = length

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = false
    }

    object IntegerType : FieldType() {

        override val type: Type
            get() = Type.INTEGER

        override val size: Int
            get() = 10

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

    object SmallintType : FieldType() {

        override val type: Type
            get() = Type.SMALLINT

        override val size: Int
            get() = 5

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

    object BigintType : FieldType() {

        override val type: Type
            get() = Type.BIGINT

        override val size: Int
            get() = 19

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

    object BooleanType : FieldType() {

        override val type: Type
            get() = Type.BOOLEAN

        override val size: Int
            get() = 1

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }


// Numeric with total length and number of digits (a.k.a. NUMERIC)
    data class DecimalType(val length: Int, val precision: Int) : FieldType() {

        override val type: Type
            get() = Type.DECIMAL

        override val size: Int
            get() = length

        override val digits: Int
            get() = precision

        override val fixedSize: Boolean
            get() = false
    }

    object FloatType : FieldType() {

        override val type: Type
            get() = Type.FLOAT

        override val size: Int
            get() = 19

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

    object DoubleType : FieldType() {

        override val type: Type
            get() = Type.DOUBLE

        override val size: Int
            get() = 19

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

// Year, month, day, hour, minutes, seconds
    object TimeStampType : FieldType() {

        override val type: Type
            get() = Type.TIMESTAMP

        override val size: Int
            get() = 14

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

// Year, month, day
    object DateType : FieldType() {

        override val type: Type
            get() = Type.DATE

        override val size: Int
            get() = 8

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

// hour, minutes, seconds
    object TimeType : FieldType() {

        override val type: Type
            get() = Type.TIME

        override val size: Int
            get() = 6

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

// Binary with fixed length
    data class BinaryType(val length: Int) : FieldType() {

        override val type: Type
            get() = Type.BINARY

        override val size: Int
            get() = length

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }

// Binary with varying length
    data class VarbinaryType(val length: Int) : FieldType() {

        override val type: Type
            get() = Type.VARBINARY

        override val size: Int
            get() = length

        override val digits: Int
            get() = 0

        override val fixedSize: Boolean
            get() = true
    }
