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

package com.smeup.dbnative.nosql

import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import com.smeup.dbnative.nosql.utils.buildInsertCommand
import com.smeup.dbnative.utils.getField
import com.smeup.dbnative.utils.matchFileKeys
import org.bson.Document

class NoSQLDBFile(override var name: String,
                  override var fileMetadata: FileMetadata,
                  private val database: MongoDatabase,
                  override var logger: Logger? = null): DBFile {

    private var globalCursor: MongoCursor<Document>? = null
    private var up_direction: Boolean = true
    private var last_set_keys: List<RecordField> = emptyList()
    private var IncludeFirst: Boolean = true
    private var lastSetOperation: Boolean = false
    private var eof: Boolean = false

    override fun eof(): Boolean {
        return eof
    }

    override fun equal(): Boolean {
        if (lastSetOperation == false || globalCursor == null) {
            return false
        } else {
            if (matchKeys(globalCursor!!.next(), last_set_keys)) {
                return true
            } else {
                return false
            }
        }
    }


    override fun setll(key: String): Boolean {
        return setll(mutableListOf(key))
    }

    /*
    Create cursor on first occourence of passed keys (up sorted list)
     */
    override fun setll(keys: List<String>): Boolean {
        eof = false

        var keyAsRecordField = keys.mapIndexed { index, value ->
            val keyname = fileMetadata.fileKeys.get(index)
            RecordField(keyname, value)
        }

        /*
        Passed keys are not primary key for DBFile
         */
        if (fileMetadata.matchFileKeys(keyAsRecordField) == false) {
            return false
        }


        /*
        Find syntax

        $and: [ { key1: { $eq: "value1" } }, { key2: { $gte: "value2" } } ] }

        example

        {$and:[{ NAZ: { $eq: "IT" } }, { REG: { $eq: "LOM" } }, { PROV: { $eq: "BS" } }, { CITTA: { $gt: "ERBUSCO" } } ] }

        */

        /*
        val filter = StringBuilder()

        val keyFields = mutableListOf<DBField>()

        keys.forEach {
            val field = fileMetadata.getField(it.name)
            if (field != null) {
                keyFields.add(field)
            }
        }

        keyFields.forEachIndexed{index: Int, dbField: DBField ->
            if (index != keys.size-1) {
                filter.append("{ ${dbField.name}: {\$eq: \"${keys.get(keys.indexOfFirst { recordField -> recordField.name == dbField.name }).value}\" } }, ")
            } else {
                filter.append("{ ${dbField.name}: {\$gte: \"${keys.get(keys.indexOfFirst { recordField -> recordField.name == dbField.name }).value}\" } }")
            }
        }
         */

        /*
        Sort sintax:

        {key1: 1, key2: -1, ....}

         */

        /*
        val sort = StringBuilder()
        fileMetadata.fields.filter { it.primaryKey }.joinTo(sort, separator=",", prefix="{", postfix="}") {
            "${it.name}: 1"
        }

        val query = "{ \$and:[ $filter ]}"

        println(query)

        var result = false

        var cursor = database.getCollection(fileMetadata.tableName).find(Document.parse(query))

        cursor = setSorting(cursor, true)
        up_direction = true
        last_keys = keys

        */

        val cursor = calculateCursor(keyAsRecordField, true, true)
        var result = false
        if (cursor.iterator().hasNext()) {
            globalCursor = cursor.iterator()
            result = true
        }

        up_direction = true
        last_set_keys = keyAsRecordField
        IncludeFirst = true

        return result
    }

    override fun setgt(key: String): Boolean {
        return setgt(mutableListOf(key))
    }

    /*
    Create cursor on first occourence of passed keys (up sorted list)
     */
    override fun setgt(keys: List<String>): Boolean {
        eof = false

        var keyAsRecordField = keys.mapIndexed { index, value ->
            val keyname = fileMetadata.fileKeys.get(index)
            RecordField(keyname, value)
        }

        /*
        Passed keys are not primary key for DBFile
         */
        if (fileMetadata.matchFileKeys(keyAsRecordField) == false) {
            return false
        }

        /*
        Find syntax

        {$and: [ { key1: { $gte: "value1" } }, { key2: { $gte: "value2" } } ] }

        example

        {$and:[{ NAZ: { $gte: "IT" } }, { REG: { $gte: "LOM" } }, { PROV: { $gte: "BS" } }, { CITTA: { $gte: "ERBUSCO" } } ] }

        *** SETGT forward:

        {$and:[{ NAZ: { $eq: "IT" } }, { REG: { $eq: "LOM" } }, { PROV: { $eq: "BS" } }, { CITTA: { $gt: "ERBUSCO" } } ] }

        with order {NAZ: 1, REG: 1, PROV: 1, CITTA: 1}

        ***SETGT backward:

        {$and:[{ NAZ: { $eq: "IT" } }, { REG: { $eq: "LOM" } }, { PROV: { $eq: "BS" } }, { CITTA: { $gt: "ERBUSCO" } } ] }

        with order {NAZ: 1, REG: 1, PROV: 1, CITTA: -1}

        */

        /*
        val filter = StringBuilder()
        val primaryFields = fileMetadata.fields.filter { it.primaryKey }

        primaryFields.forEachIndexed{index: Int, dbField: DBField ->
            if (index != primaryFields.size-1) {
                filter.append("{ ${dbField.name}: {\$eq: \"${keys.get(keys.indexOfFirst { recordField -> recordField.name == dbField.name }).value}\" } }, ")
            } else {
                filter.append("{ ${dbField.name}: {\$gt: \"${keys.get(keys.indexOfFirst { recordField -> recordField.name == dbField.name }).value}\" } }")
            }
        }
        */


        /*
        Sort sintax:

        {key1: 1, key2: -1, ....}

        example

        {NAZ: 1, REG: 1, PROV: 1, CITTA: 1}

         */

        /*
        val sort = StringBuilder()
        fileMetadata.fields.filter { it.primaryKey }.joinTo(sort, separator=",", prefix="{", postfix="}") {
            "${it.name}: 1"
        }

        val query = "{ \$and: [ $filter ]}"

        println(query)

        var result = false

        var cursor = database.getCollection(fileMetadata.tableName).find(Document.parse(query))

        cursor = setSorting(cursor, true)
         */

        val cursor = calculateCursor(keyAsRecordField, true, includeFirst = false)

        var result = false

        if (cursor.iterator().hasNext()) {
            globalCursor = cursor.iterator()
            result = true
        }

        up_direction = true
        last_set_keys = keyAsRecordField
        IncludeFirst = false

        return result
    }

    override fun chain(key: String): Result {

        return chain(mutableListOf(key))
    }

    override fun chain(keys: List<String>): Result {
        eof = false

        var keyAsRecordField = keys.mapIndexed { index, value ->
            val keyname = fileMetadata.fileKeys.get(index)
            RecordField(keyname, value)
        }

        /*
        Passed keys are not primary key for DBFile
         */
        if (fileMetadata.matchFileKeys(keyAsRecordField) == false) {
            return Result(record = Record(), indicatorLO = true)
        }

        val cursor = calculateCursor(keyAsRecordField, true, true)

        globalCursor = cursor.iterator()
        up_direction = true
        last_set_keys = keyAsRecordField

        //when globalCursor is empty return result empty
        val document = globalCursor!!.tryNext() ?: return Result(Record())

        if (matchKeys(document, keyAsRecordField)) {
            val record = documentToRecord(document)
            updateLastKeys(record)
            return Result(record)
        } else {
            return Result(Record())
        }
    }

    /*
    Read next record from current cursor (up direction sorted list)
     */
    override fun read(): Result {

        if (globalCursor == null) {
            return Result(
                indicatorLO = true,
                errorMsg = "Cursor not defined. Call SETLL or SETGT before invoke READ command"
            )
        }

        if (!up_direction) {
            val cursor = calculateCursor(last_set_keys, true, !IncludeFirst)
            if (cursor.iterator().hasNext()) {
                globalCursor = cursor.iterator()
            }
            up_direction = true
        }

        IncludeFirst = true

        if (globalCursor != null) {

            if (globalCursor!!.hasNext()) {
                val record = documentToRecord(globalCursor!!.next())
                if (globalCursor!!.hasNext()) {
                    updateLastKeys(record)
                    return Result(record = record)
                } else {
                    eof = true
                    return Result(record = record, indicatorEQ = true)
                }
            } else {
                return Result(indicatorLO = true, errorMsg = "READ called on EOF cursor")
            }
        } else {
            return Result(indicatorLO = true, errorMsg = "Cursor not defined. Call SETLL or SETGT before invoke READ command")
        }
    }

    /*
    Is it like read()? Not sure...
     */
    override fun readEqual(): Result {
        return read()
    }

    override fun readEqual(key: String): Result {
        return readEqual(mutableListOf(key))
    }

    /*
    For READE command explanation see:

    https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_71/rzasd/sc092508987.htm
     */
    override fun readEqual(keys: List<String>): Result {

        var keyAsRecordField = keys.mapIndexed { index, value ->
            val keyname = fileMetadata.fileKeys.get(index)
            RecordField(keyname, value)
        }

        if (globalCursor == null) {
            globalCursor = calculateCursor(keyAsRecordField, true, true).iterator()
        }

        /*
        Passed keys are not primary key for DBFile
         */
        if (fileMetadata.matchFileKeys(keyAsRecordField) == false) {
            return Result(indicatorLO = true, errorMsg = "READE keys not matching file primary keys")
        }

        if (!up_direction) {
            val cursor = calculateCursor(last_set_keys, true, !IncludeFirst)
            if (cursor.iterator().hasNext()) {
                globalCursor = cursor.iterator()
            }
            up_direction = true
        }

        IncludeFirst = true

        while (true) {
            if (globalCursor!!.hasNext()) {

                val document = globalCursor!!.next()
                val record = documentToRecord(document)

                updateLastKeys(record)

                if (matchKeys(document, keyAsRecordField)) {
                    return Result(record = record)
                } else {
                    eof = true
                    return Result(indicatorHI = true)
                }
            } else {
                eof = true
                // End of data and no match found, reset cursor
                return Result(indicatorLO = true, errorMsg = "READ called on EOF cursor")
            }
        }
    }

    override fun readPrevious(): Result {
        if (globalCursor == null) {
            return Result(
                indicatorLO = true,
                errorMsg = "Cursor not defined. Call SETLL or SETGT before invoke READ command"
            )
        }

        if (up_direction) {
            val cursor = calculateCursor(last_set_keys, false, !IncludeFirst)
            if (cursor.iterator().hasNext()) {
                globalCursor = cursor.iterator()
            }
            up_direction = false
        }

        IncludeFirst = true

        if (globalCursor != null) {

            if (globalCursor!!.hasNext()) {
                val record = documentToRecord(globalCursor!!.next())
                if (globalCursor!!.hasNext()) {
                    updateLastKeys(record)
                    return Result(record = record)
                } else {
                    eof = true
                    return Result(record = record, indicatorEQ = true)
                }
            } else {
                return Result(indicatorLO = true, errorMsg = "READ called on EOF cursor")
            }
        } else {
            return Result(indicatorLO = true, errorMsg = "Cursor not defined. Call SETLL or SETGT before invoke READ command")
        }
    }

    override fun readPreviousEqual(): Result {
        return readPrevious()
    }

    override fun readPreviousEqual(key: String): Result {
        return readPreviousEqual(mutableListOf(key))
    }

    override fun readPreviousEqual(keys: List<String>): Result {

        var keyAsRecordField = keys.mapIndexed { index, value ->
            val keyname = fileMetadata.fileKeys.get(index)
            RecordField(keyname, value)
        }

        if (globalCursor == null) {
            return Result(
                indicatorLO = true,
                errorMsg = "Cursor not defined. Call SETLL or SETGT before invoke READ command"
            )
        }

        /*
        Passed keys are not primary key for DBFile
         */
        if (fileMetadata.matchFileKeys(keyAsRecordField) == false) {
            return Result(indicatorLO = true, errorMsg = "READE keys not matching file primary keys")
        }

        if (up_direction) {
            val cursor = calculateCursor(last_set_keys, false, !IncludeFirst)

            if (cursor.iterator().hasNext()) {
                globalCursor = cursor.iterator()
            }
            up_direction = false
        }

        IncludeFirst = true


        while (true) {
            if (globalCursor!!.hasNext()) {

                val document = globalCursor!!.next()
                val record = documentToRecord(document)

                updateLastKeys(record)

                if (matchKeys(document, keyAsRecordField)) {
                    return Result(record = record)
                } else {
                    eof = true
                    return Result(indicatorHI = true)
                }
            } else {
                eof = true
                // End of data and no match found, reset cursor
                return Result(indicatorLO = true, errorMsg = "READ called on EOF cursor")
            }
        }
    }


    override fun write(record: Record): Result {
        val insertCommand = fileMetadata.buildInsertCommand(fileMetadata.tableName, record)
        println(insertCommand)
        executeCommand(insertCommand)
        return Result(record = record)
    }

    override fun update(record: Record): Result {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun delete(record: Record): Result {
        TODO("Not yet implemented")
    }

    private fun executeCommand(command: String) = database.runCommand(Document.parse(command))

    /*
    Evaluate matching between values of passed keys and relative values in document
     */
    private fun matchKeys(document: Document, keys: List<RecordField>): Boolean {

        var match = true

        keys.forEach({
            if (it.value == document.getString(it.name) == false) {
                match = false
            }
        })

        return match

    }


    private fun documentToRecord(document: Document?): Record {
        val result = Record()

        if (document != null) {
            fileMetadata.fields.forEach({
                result.add(RecordField(it.name, document.getString(it.name)))
            })
        }
        return result
    }

    private fun calculateCursor(keys: List<RecordField>, up_direction: Boolean, includeFirst: Boolean): FindIterable<Document> {
        /*

        Complete examples for filters in file filter_syntax_examples.txt

        Filter format:

        {$or:[

        {$and:[{ NAZ: { <operator3> "IT" } }, { REG: { <operator3> "LOM" } }, { PROV: { <operator3> "BS" } }, { CITTA: { <operator1> "ZONE" } } ] },

        {$and:[{ NAZ: { <operator3> "IT" } }, { REG: { <operator3> "LOM" } }, { PROV: { <operator2> "BS" } } ] },

        {$and:[{ NAZ: { <operator3> "IT" } }, { REG: { <operator2> "LOM" } } ] },

        { NAZ: { <operator2> "IT" } }

        ] }

        Example for UP and SETLL:

        {$or:[

        {$and:[{ NAZ: { $gte: "IT" } }, { REG: { $gte: "LOM" } }, { PROV: { $gte: "BS" } }, { CITTA: { $gte: "ZONE" } } ] },

        {$and:[{ NAZ: { $gte: "IT" } }, { REG: { $gte: "LOM" } }, { PROV: { $gt: "BS" } } ] },

        {$and:[{ NAZ: { $gte: "IT" } }, { REG: { $gt: "LOM" } } ] },

        { NAZ: { $gt: "IT" } }

        ] }

        */

        var operator1: MongoOperator = MongoOperator.EQ
        var operator2: MongoOperator = MongoOperator.EQ
        var operator3: MongoOperator = MongoOperator.EQ

        when {
            up_direction && includeFirst -> {
                operator1 = MongoOperator.GE
                operator2 = MongoOperator.GT
                operator3 = MongoOperator.GE
            }

            !up_direction && includeFirst -> {
                operator1 = MongoOperator.LE
                operator2 = MongoOperator.LT
                operator3 = MongoOperator.LE
            }

            up_direction && !includeFirst -> {
                operator1 = MongoOperator.GT
                operator2 = MongoOperator.GT
                operator3 = MongoOperator.GE
            }

            !up_direction && !includeFirst -> {
                operator1 = MongoOperator.LT
                operator2 = MongoOperator.LT
                operator3 = MongoOperator.LE
            }
            else -> {}
        }

        val keyFields = mutableListOf<Field>()

        keys.forEach {
            val field = fileMetadata.getField(it.name)
            if (field != null) {
                keyFields.add(field)
            }
        }


        val filter = StringBuilder()

        filter.append("{\$or:[")

        val orContent = StringBuilder()


        // Add first line
        val line = StringBuilder()

        line.append("{\$and:[")

        keyFields.forEachIndexed{index: Int, dbField: Field ->
            if (index != keyFields.size-1) {
                line.append("{ \"${dbField.name}\": {${operator3.symbol} \"${keys.get(keys.indexOfFirst { recordField -> recordField.name == dbField.name }).value}\" } }, ")
            }
            else {
                line.append("{ \"${dbField.name}\": {${operator1.symbol} \"${keys.get(keys.indexOfFirst { recordField -> recordField.name == dbField.name }).value}\" } }")
            }
        }

        line.append("] }")

        orContent.append(line.toString())

        // Add other lines
        if (keyFields.size > 1) {

            var while_index = keyFields.size

            while (while_index > 1) {

                val tempLine = StringBuilder()

                tempLine.append(", {\$and:[")

                val subList = keyFields.subList(0, while_index-1)

                subList.forEachIndexed{index: Int, dbField: Field ->
                    if (index != subList.size-1) {
                        tempLine.append("{ \"${dbField.name}\": {${operator3.symbol} \"${keys.get(keys.indexOfFirst { recordField -> recordField.name == dbField.name }).value}\" } }, ")
                    }
                    else {
                        tempLine.append("{ \"${dbField.name}\": {${operator2.symbol} \"${keys.get(keys.indexOfFirst { recordField -> recordField.name == dbField.name }).value}\" } }")
                    }
                }

                tempLine.append("] }")

                orContent.append(tempLine.toString())

                while_index--
            }
        }

        filter.append(orContent.toString())

        filter.append("] }")


        /*
        Sort sintax:

        {key1: 1, key2: -1, ....}

         */

        val sort = StringBuilder()

        keyFields.joinTo(sort, separator= ",", prefix= "{", postfix = "}") {
            if (up_direction) {
                "\"${it.name}\": 1"
            } else {
                "\"${it.name}\": -1"
            }
        }
        sort.append("}")

        println("$filter - $sort")

        val cursor = database.getCollection(fileMetadata.tableName).find(Document.parse(filter.toString()))

        return cursor.sort(Document.parse(sort.toString()))
    }

    private fun updateLastKeys(record: Record) {

        val lastKeys = mutableListOf<RecordField>()
        fileMetadata.fileKeys.forEach {
            lastKeys.add(RecordField(it, record.getValue(it)))
        }
        last_set_keys = lastKeys
    }

    enum class MongoOperator(val symbol: String) {
        EQ("\$eq:"),
        NE("\$neq:"),
        GT("\$gt:"),
        GE("\$gte:"),
        LT("\$lt:"),
        LE("\$lte:")
    }

}

