package com.smeup.dbnative

/**
 * Resolves the most specific [ConnectionConfig] matching [fileName].
 */
fun findConnectionConfigFor(fileName: String, connectionsConfig: List<ConnectionConfig>): ConnectionConfig {
    val configList = connectionsConfig.filter {
        it.fileName.equals(fileName, ignoreCase = true) || it.fileName == "*" ||
            fileName.uppercase().matches(Regex(it.fileName.uppercase().replace("*", ".*")))
    }
    require(configList.isNotEmpty()) {
        "Wrong configuration. Not found a ConnectionConfig entry matching name: $fileName"
    }
    return configList.sortedWith(ConnectionConfigComparator())[0]
}

/**
 * Orders connection patterns from most specific to least specific.
 */
class ConnectionConfigComparator : Comparator<ConnectionConfig> {
    override fun compare(o1: ConnectionConfig?, o2: ConnectionConfig?): Int {
        require(o1 != null)
        require(o2 != null)
        return when {
            o1.fileName == "*" && o2.fileName != "*" -> 1
            o1.fileName != "*" && o2.fileName == "*" -> -1
            o1.fileName.contains("*") && o2.fileName.contains("*") -> o1.fileName.compareTo(o2.fileName)
            o1.fileName.contains("*") && !o2.fileName.contains("*") -> 1
            !o1.fileName.contains("*") && o2.fileName.contains("*") -> -1
            else -> o1.fileName.compareTo(o2.fileName)
        }
    }
}
