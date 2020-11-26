package com.smeup.dbnative.jt400

import com.ibm.as400.access.*
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.model.FileMetadata

open class JT400DBMMAnager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl()  {

    private var openedFile = mutableMapOf<String, JT400DBFile>()

    val connection : AS400 by lazy {
        val as400 = AS400(connectionConfig.url, connectionConfig.user, connectionConfig.password)
        as400.isGuiAvailable = false
        //as400.addConnectionListener
        as400.connectService(AS400.RECORDACCESS)
        as400
    }

    override fun openFile(name: kotlin.String) = openedFile.getOrPut(name) {
            //: com.smeup.dbnative.file.DBFile {
        require(existFile(fileNameFromPath(name))) {
            "Cannot open unregistered file $name"
        }
        //
        println(name)
        //name must be a QSYS path like "/QSYS.LIB/library.LIB/filename.FILE/*FILE.MBR"
        val fileName =
            QSYSObjectPathName(name)
            //QSYSObjectPathName(library, name, "*FILE", "MBR")
        val path = fileName.path
        val file = KeyedFile(connection, path)
        //val rf = AS400FileRecordDescription(system, path).retrieveRecordFormat()
        //file.recordFormat = rf[0]
        file. setRecordFormat() // Loads the record format directly from the server.
        //TODO("non sempre deve essere read-only")
        file.open(AS400File.READ_ONLY, 0, AS400File.COMMIT_LOCK_LEVEL_NONE)
        return JT400DBFile(name,
            metadataOf(fileNameFromPath(name))
            , file)
    }

    fun fileNameFromPath(path: String) : String {
        val name = path.substringBefore(".FILE/").substringAfterLast("/")
        println(name)
        return name
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