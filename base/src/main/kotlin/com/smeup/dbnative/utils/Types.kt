package com.smeup.dbnative.utils

import com.smeup.dbnative.model.*
import com.smeup.dbnative.model.FileMetadata

data class TypedField(val field: Field, val type: FieldType){
}

fun Collection<TypedField>.fieldList():List<Field>{
    return map({tf -> tf.field})
}

fun Collection<TypedField>.fieldTypeList():List<FieldType>{
    return map({tf -> tf.type})
}

data class TypedMetadata(var tableName: String,
                    var recordFormat: String,
                    var fields: List<TypedField>,
                    var fileKeys:List<String>,
                    var unique:Boolean = false){
    fun fileMetadata(): FileMetadata = FileMetadata(tableName, recordFormat, fields.fieldList(), fileKeys, unique);

    fun fieldsToProperties(): MutableList<Pair<String, String>>{
        val properties = mutableListOf<Pair<String, String>>()

        for ((index, field) in fields.iterator().withIndex()) {
            properties.add(
                Pair(
                    "field.${field.field.name}",
                    "${field.field.text},${fields[index].type.type},${fields[index].type.size},${fields[index].type.digits}"
                )
            )
        }
        return properties;
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

fun String.getFieldTypeInstance(columnSize: Int, decimalDigits: Int): FieldType {

    val fieldTypeObject = when (this.toUpperCase()) {
        "CHAR","CHARACTER" -> CharacterType(columnSize)
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