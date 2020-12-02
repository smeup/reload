package com.smeup.dbnative.jt400

import com.ibm.as400.access.*
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.model.FileMetadata

open class JT400DBMMAnager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl()  {

    private var openedFile = mutableMapOf<String, JT400DBFile>()

    //   as400://SRVLAB01.SMEUP.COM/W_SCAARM
    private val match = Regex("as400://((?:\\w|\\.)+)/(\\w+)").find(connectionConfig.url)
    private val host : String by lazy {
        match!!.destructured.component1()
    }
//    private val port : Int by lazy {
//        match!!.destructured.component2().toInt()
//    }
    private val library : String by lazy {
        match!!.destructured.component2()
    }

    val connection : AS400 by lazy {
        val as400 = AS400(host, connectionConfig.user, connectionConfig.password)
        as400.isGuiAvailable = false
        //as400.addConnectionListener
        as400.connectService(AS400.RECORDACCESS)
        as400
    }

    override fun openFile(name: kotlin.String) = openedFile.getOrPut(name) {
            //: com.smeup.dbnative.file.DBFile {
        require(existFile(name)) {
            "Cannot open unregistered file $name"
        }
        //
        val fileName = QSYSObjectPathName(library, name, "*FILE", "MBR")
        val path = fileName.path
        println("Path: " + path)
        val file = KeyedFile(connection, path)
        //val rf = AS400FileRecordDescription(system, path).retrieveRecordFormat()
        //file.recordFormat = rf[0]
        file. setRecordFormat() // Loads the record format directly from the server.
        //TODO("non sempre deve essere read-only")
        file.open(AS400File.READ_WRITE, 0, AS400File.COMMIT_LOCK_LEVEL_NONE)
        return JT400DBFile(name, metadataOf(name), file)
    }

    override fun closeFile(name: String) {
        val file = openedFile[name]
        if (file != null) {
            file.close()
            openedFile.remove(name)
        }
    }

    override fun validateConfig() {
        TODO("Not yet implemented")
    }

    override fun close() {
        connection.disconnectAllServices()
    }
}