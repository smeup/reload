package com.smeup.dbnative.sql

import com.smeup.dbnative.file.Record
import java.lang.Exception

enum class PositioningMethod {
    SETLL, SETGT;
}

enum class ReadMethod(val forward: Boolean){
    READE(true), READPE(false), READP(false), READ(true), CHAIN(true);
}

enum class SortOrder(val symbol: String){
    ASCEDING("ASC"), DESCENDING("DESC");
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

class Native2SQL(val fileKeys: List<String>, val tableName: String) {
    private var lastPositioningInstruction: PositioningInstruction? = null
    private var lastReadInstruction: ReadInstruction? = null

    fun checkPositioning(){
        requireNotNull(lastPositioningInstruction){
            "No positioning instruction found"
        }
    }

    fun checkRead(){
        requireNotNull(lastReadInstruction){
            "No read instruction found"
        }
    }

    fun checkReadKeys(){
        checkRead()
        require(!lastReadInstruction!!.keys.isNullOrEmpty()){
            "No keys specified for read instruction ${lastReadInstruction!!.method}"
        }
    }

    fun checkInstructions(){
        checkPositioning()
        checkRead()
    }

    fun setPositioning(method: PositioningMethod, keys: List<String>){
        lastPositioningInstruction = PositioningInstruction(method, keys)
        lastReadInstruction = null
    }

    /*
     * @return true if read method need new query execution
     */
    fun setRead(method: ReadMethod, keys: List<String>? = null): Boolean{
        var executeQuery = false
        when(method){
            ReadMethod.READPE, ReadMethod.READE ->{
                val coerent = keys?.let{isCoerent(keys)} ?: true
                //Test to remove on fully supported operations
                require(coerent){
                    "Uncoerent read not yet managed"
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
        if(lastReadInstruction == null){
            executeQuery = true
        }
        lastReadInstruction = ReadInstruction(method, keys ?:emptyList())
        return executeQuery
    }

    fun clear(){
        lastReadInstruction = null
        lastPositioningInstruction = null
    }

    fun getLastKeys() = lastReadInstruction?.keys ?:lastPositioningInstruction?.keys ?: throw Exception("Keys not yet set")

    fun isLastOperationSet()=lastReadInstruction == null

    fun lastReadMatchRecord(record: Record): Boolean {
        if(lastReadInstruction!!.keys.isEmpty()){
            return true
        }
        lastReadInstruction!!.keys.mapIndexed { index, value ->
            val keyname = fileKeys.get(index)
            if(record[keyname] != value){
                return false
            }
        }
        return true
    }


    fun isCoerent(newKeys: List<String>): Boolean{
        checkPositioning()
        return newKeys.isEmpty() ||
            if (newKeys.size <= lastPositioningInstruction!!.keys.size &&
            newKeys.size <= lastReadInstruction?.keys?.size?:newKeys.size) {
            newKeys.forEachIndexed() { index, value ->
                if(lastPositioningInstruction!!.keys.get(index) != value){
                    return false
                }
            }
            return true
        }
        else false
    }


    private fun getSortOrder(): SortOrder {
        checkInstructions()
        return if(lastReadInstruction!!.method.forward) SortOrder.ASCEDING else SortOrder.DESCENDING
    }

    private fun getComparison(): Pair<Comparison, Comparison>{
        checkInstructions()
        return if(lastReadInstruction!!.method.forward){
            when(lastPositioningInstruction!!.method){
                        PositioningMethod.SETLL -> return Pair(Comparison.GE, Comparison.GT)
                        PositioningMethod.SETGT -> return Pair(Comparison.GT, Comparison.GT)
            }
        }
        else{
            when(lastPositioningInstruction!!.method){
                PositioningMethod.SETLL -> return Pair(Comparison.LT, Comparison.LT)
                PositioningMethod.SETGT -> return Pair(Comparison.LE, Comparison.LT)
            }
        }
    }

    private fun getSQLOrderByClause(): String{
        val sortOrder = getSortOrder()
        return " ORDER BY " + fileKeys.joinToString(separator = " " + sortOrder.symbol + ", ", postfix = " " + sortOrder.symbol )
    }

    fun getReadSqlStatement(): Pair<String, List<String>>{
        checkPositioning()
        return Pair(getSQL(fileKeys.subList(0, lastPositioningInstruction!!.keys.size), Comparison.EQ, tableName), lastPositioningInstruction!!.keys)
    }

    fun getSQLSatement(): Pair<String, List<String>>{
        when(lastReadInstruction!!.method){
            ReadMethod.CHAIN ->{
                checkReadKeys()
                return Pair(getSQL(fileKeys.subList(0, lastReadInstruction!!.keys.size), Comparison.EQ, tableName), lastReadInstruction!!.keys)
            }
            ReadMethod.READ,ReadMethod.READP -> {
                checkRead()
                return getCoerentSql(true)
            }
            else -> {
                checkReadKeys()
                return getCoerentSql()
            }
        }
    }


    private fun getCoerentSql(fullUnion: Boolean = false):Pair<String, List<String>>{
        checkPositioning()
        val queries = mutableListOf<String>()
        val replacements = mutableListOf<String>()
        val comparison = getComparison()
        require(!lastPositioningInstruction!!.keys.isEmpty()){
            "Empty positioning keys"
        }
        if(lastPositioningInstruction!!.keys.size == 1){
            queries.add(getSQL(fileKeys, comparison.first, tableName))
            replacements.addAll(lastPositioningInstruction!!.keys)
        }
        else {
            val limit = if(fullUnion)1 else 2
            for (i in lastPositioningInstruction!!.keys.size downTo limit) {
                queries.add(getSQL(fileKeys.subList(0, i), if(i == lastPositioningInstruction!!.keys.size) comparison.first else comparison.second, tableName))
                replacements.addAll(lastPositioningInstruction!!.keys.subList(0, i))
            }
        }
        return Pair(queries.joinToString(" UNION ") + getSQLOrderByClause(), replacements)
    }
}

private fun getSQL(keys: List<String>, comparison: Comparison, tableName: String): String{
    var value = ""
    keys.forEachIndexed { index, k ->
        run {
            value += k + " " + (if (index < keys.size - 1) Comparison.EQ.symbol else  comparison.symbol) + " ? AND "
        }
    }
    return "(SELECT * FROM $tableName WHERE " + value.removeSuffix("AND ") + ")"
}

fun main(){
    val adapter = Native2SQL(listOf("Regione", "Provincia", "Comune"), "rld_comuni").apply{
        setPositioning(PositioningMethod.SETLL, listOf("Lombardia", "Brescia", "Erbusco"))
        println(isCoerent(listOf("Lombardia", "Brescia", "Erbusco")))
        setRead(ReadMethod.READE, listOf("Lombardia", "Brescia", "Erbusco"))
    }
    adapter.getSQLSatement().let{
        println(it.first)
        println(it.second)
    }
}