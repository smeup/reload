package com.smeup.dbnative.nosql.utils

import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import java.lang.Exception

enum class PositioningMethod {
    SETLL, SETGT;
}

enum class ReadMethod(val forward: Boolean) {
    READE(true), READPE(false), READP(false), READ(true), CHAIN(true);
}

class PositioningInstruction(val method: PositioningMethod, val keys: List<String>){
    init {
        require(keys.isNotEmpty()){
            "No keys specified for positioning instruction $method"
        }
    }
}

class ReadInstruction(var method: ReadMethod, var keys: List<String>){
    constructor(method: ReadMethod): this(method, emptyList()){
        require(admitEmptyKeys()){
            "Keys are mandatory for read instruction $method "
        }
    }

    fun admitEmptyKeys() = method == ReadMethod.READ || method == ReadMethod.READP
}

////
enum class SortOrder(val symbol: String){
    ASCEDING("1"), DESCENDING("-1");
}

class Native2Mongo(val fileKeys: List<String>, val tableName: String) {
    private var lastPositioningInstruction: PositioningInstruction? = null
    private var lastReadInstruction: ReadInstruction? = null

    private fun checkPositioning() {
        requireNotNull(lastPositioningInstruction) {
            "No positioning instruction found"
        }
    }

    private fun checkKeys(keys: List<String>) {
        require(fileKeys.isNotEmpty()) {
            "No keys specified in metadata"
        }
        require(keys.size <= fileKeys.size) {
            "Number of metadata keys $fileKeys less than number of positioning/read keys $keys"
        }
    }

    private fun checkRead() {
        requireNotNull(lastReadInstruction) {
            "No read instruction found"
        }
    }

    private fun checkReadKeys() {
        checkRead()
        require(!lastReadInstruction!!.keys.isNullOrEmpty()) {
            "No keys specified for read instruction ${lastReadInstruction!!.method}"
        }
    }

    private fun checkInstructions() {
        checkPositioning()
        checkRead()
    }

    fun setPositioning(method: PositioningMethod, keys: List<String>) {
        checkKeys(keys)
        lastPositioningInstruction = PositioningInstruction(method, keys)
        lastReadInstruction = null
    }

    /*
     * @return true if read method need new query execution
     */
    fun setRead(method: ReadMethod, keys: List<String>? = null): Boolean {
        checkKeys(keys ?: emptyList())
        var executeQuery = false
        when (method) {
            ReadMethod.READPE, ReadMethod.READE -> {
                val coherent = keys?.let { isCoherent(keys) } ?: true
                //Test to remove on fully supported operations
                require(coherent) {
                    "Uncoherent read not yet managed"
                }
            }
            ReadMethod.READ, ReadMethod.READP -> checkPositioning()
            ReadMethod.CHAIN -> {
                lastPositioningInstruction = null
                executeQuery = true
            }
        }
        require(lastReadInstruction == null || method == lastReadInstruction!!.method) {
            "read operations are only allowed immediatly after positioning or after a same method read instruction"
        }
        if (lastReadInstruction == null) {
            executeQuery = true
        }
        lastReadInstruction = ReadInstruction(method, keys ?: emptyList())
        return executeQuery
    }

    fun clear() {
        lastReadInstruction = null
        lastPositioningInstruction = null
    }

    fun getLastKeys() =
        lastReadInstruction?.keys ?: lastPositioningInstruction?.keys ?: throw Exception("Keys not yet set")

    fun isLastOperationSet() = lastReadInstruction == null

    fun lastReadMatchRecord(record: Record): Boolean {
        if (lastReadInstruction!!.keys.isEmpty()) {
            return true
        }
        lastReadInstruction!!.keys.mapIndexed { index, value ->
            val keyname = fileKeys.get(index)
            if (record[keyname]?.trim() != value.trim()) {
                return false
            }
        }
        return true
    }

    private fun getSortOrder(): SortOrder {
        checkInstructions()
        return if (lastReadInstruction!!.method.forward) SortOrder.ASCEDING else SortOrder.DESCENDING
    }

    private fun getComparison(): Pair<MongoOperator, MongoOperator>{
        checkInstructions()
        return if(lastReadInstruction!!.method.forward){
            when(lastPositioningInstruction!!.method){
                PositioningMethod.SETLL -> return Pair(MongoOperator.GE, MongoOperator.GT)
                PositioningMethod.SETGT -> return Pair(MongoOperator.GT, MongoOperator.GT)
            }
        }
        else{
            when(lastPositioningInstruction!!.method){
                PositioningMethod.SETLL -> return Pair(MongoOperator.LT, MongoOperator.LT)
                PositioningMethod.SETGT -> return Pair(MongoOperator.LE, MongoOperator.LT)
            }
        }
    }

    private fun getSort(): String {
        val sort = StringBuilder()
        val so = getSortOrder()
        fileKeys.joinTo(sort, separator = ",", prefix = "{", postfix = "}") {
            "\"$it\": ${so.symbol}"
        }
        sort.append("}")
        return sort.toString()
    }

    fun isCoherent(newKeys: List<String>): Boolean {
        checkPositioning()
        return newKeys.isEmpty() ||
                if (newKeys.size <= lastPositioningInstruction!!.keys.size &&
                    newKeys.size <= lastReadInstruction?.keys?.size ?: newKeys.size
                ) {
                    newKeys.forEachIndexed() { index, value ->
                        if (lastPositioningInstruction!!.keys.get(index) != value) {
                            return false
                        }
                    }
                    return true
                } else false
    }
    /////////////////////

    private fun readInstructionToRecordField():List<RecordField>{
        checkRead()
        return  lastReadInstruction!!.keys.mapIndexed { index, value ->
            val keyname = fileKeys.get(index)
            RecordField(keyname, value)
        }
    }

    private fun positioninfInstructionToRecordField():List<RecordField>{
        checkRead()
        return  lastPositioningInstruction!!.keys.mapIndexed { index, value ->
            val keyname = fileKeys.get(index)
            RecordField(keyname, value)
        }
    }


    private fun getFilterForChain(): String {
        val filter = StringBuilder()
        val keys = readInstructionToRecordField()
        if (keys.size > 1) {
            filter.append("{\$and:[")
        }
        keys.forEach { k -> filter.append("{\"${k.name}\": {${MongoOperator.EQ.symbol} \"${k.value}\"}}, ") }
        if (keys.size > 1) {
            filter.append("]}")
        }
        return filter.toString()
    }


    fun getQuery(): Pair<String, String>{
        when(lastReadInstruction!!.method){
            ReadMethod.CHAIN ->{
                checkReadKeys()
                return Pair(getFilterForChain(), "")
            }
            ReadMethod.READ,ReadMethod.READP -> {
                checkRead()
                return Pair(getCoherentFilters(true), getSort())
            }
            else -> {
                checkReadKeys()
                return Pair(getCoherentFilters(false), getSort())
            }
        }
    }

    fun getReadQuery(): Pair<String, String>{
        return Pair("","")
    }

    private fun getCoherentFilters(full: Boolean = false):String{
        checkPositioning()
        val queries = mutableListOf<String>()
        val comparison = getComparison()
        require(!lastPositioningInstruction!!.keys.isEmpty()){
            "Empty positioning keys"
        }
        if(lastPositioningInstruction!!.keys.size == 1){
            queries.add(getFilter(positioninfInstructionToRecordField().subList(0, 1), comparison.first))
        }
        else {
            val limit = if(full) 1 else 2
            for (i in lastPositioningInstruction!!.keys.size downTo limit) {
                queries.add(getFilter(positioninfInstructionToRecordField().subList(0, i), if(i == lastPositioningInstruction!!.keys.size) comparison.first else comparison.second))
            }
        }
        return if(queries.size == 1) queries.get(0) else "{\$or:[" + queries.joinToString(", ") + "]}"
    }

    private fun getFilter(keys: List<RecordField>, comparison: MongoOperator):String{
        val filter = StringBuilder()
        keys.forEachIndexed { i, rf ->
            run {
                val cp = if(keys.size - 1== i) comparison else MongoOperator.EQ
                filter.append("{\"${rf.name}\": {${cp.symbol} \"${rf.value}\"}}, ")
            }
        }
        return "{\$and:[" + filter.toString() + "]}"
    }
}
