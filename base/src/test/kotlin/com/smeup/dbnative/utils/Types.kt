package com.smeup.dbnative.utils

import com.smeup.dbnative.model.*
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.util.*
import kotlin.collections.ArrayList

data class TypedField(val field: Field, val type: FieldType)

fun Collection<TypedField>.fieldList(): List<Field> {
    return map({ tf -> tf.field })
}

fun Collection<TypedField>.fieldTypeList(): List<FieldType> {
    return map({ tf -> tf.type })
}

data class TypedMetadata(
    var name: String,
    var tableName: String,
    var fields: List<TypedField>,
    var fileKeys: List<String>,
) {
    fun fileMetadata(): FileMetadata = FileMetadata(name, tableName, fields.fieldList(), fileKeys)

    fun fieldsToProperties(): MutableList<Pair<String, String>> {
        val properties = mutableListOf<Pair<String, String>>()

        for ((index, field) in fields.iterator().withIndex()) {
            properties.add(
                Pair(
                    "field.${field.field.name}",
                    "${field.field.text},${fields[index].type.type},${fields[index].type.size},${fields[index].type.digits}",
                ),
            )
        }
        return properties
    }

    fun getField(name: String): TypedField? {
        return if (fields.filter { it.field.name == name }.count() > 0) {
            fields.first { it.field.name == name }
        } else {
            null
        }
    }
}

infix fun String.fieldByType(type: FieldType): TypedField = TypedField(Field(this), type)

fun String.getFieldTypeInstance(
    columnSize: Int,
    decimalDigits: Int,
): FieldType {
    val fieldTypeObject =
        when (this.uppercase()) {
            "CHAR", "CHARACTER" -> CharacterType(columnSize)
            "VARCHAR" -> VarcharType(columnSize)
            "INT", "INTEGER" -> IntegerType
            "SMALLINT" -> SmallintType
            "BIGINT" -> BigintType
            "BOOLEAN", "BOOL" -> BooleanType
            "DECIMAL" -> DecimalType(columnSize, decimalDigits)
            "DOUBLE" -> DoubleType
            "FLOAT" -> FloatType
            "TIMESTAMP" -> TimeStampType
            "TIME" -> TimeType
            "DATE" -> DateType
            "BINARY" -> BinaryType(columnSize)
            "VARBINARY" -> VarbinaryType(columnSize)
            else -> throw IllegalArgumentException("Wrong type of FieldType")
        }

    return fieldTypeObject
}

fun propertiesToTypedMetadata(
    propertiesDirPath: String,
    fileName: String,
): TypedMetadata {
    val propertiesFile = FileInputStream(File("$propertiesDirPath${File.separatorChar}${fileName.uppercase()}.properties"))
    // val properties = Properties()
    // properties.load(InputStreamReader(propertiesFile, Charset.forName("UTF-8")))

    val mp: MutableMap<String, String> = LinkedHashMap()
    object : Properties() {
        @Synchronized
        override fun put(
            key: Any,
            value: Any,
        ): Any? {
            return mp.put(key as String, value as String)
        }
    }.load(InputStreamReader(propertiesFile, Charset.forName("UTF-8")))

    // Fields
    var flds = mp.filterKeys { it.startsWith("field.") }
    val fields: MutableList<TypedField> = ArrayList()
    flds.forEach { fld ->
        val name = fld.key.split(".")[1]
        val fldAttributes = fld.value.split(",")
        val description = fldAttributes[0].trim()
        val length = fldAttributes[2].trim().toInt()
        val decimal = fldAttributes[3].trim().toInt()

        val datatype = fldAttributes[1].trim()
        val fieldType = datatype.getFieldTypeInstance(length, decimal)

        fields.add(TypedField(Field(name, description), fieldType))
    }

    // FormatName
    val tablename = mp.get("tablename") ?: ""

    // FieldKeys
    val fieldsKeys: MutableList<String> = ArrayList()
    if (!(mp.get("filekeys")).isNullOrEmpty()) {
        fieldsKeys.addAll((mp.get("filekeys")?.split(",")!!))
    }

    return TypedMetadata(fileName, tablename, fields, fieldsKeys)
}
