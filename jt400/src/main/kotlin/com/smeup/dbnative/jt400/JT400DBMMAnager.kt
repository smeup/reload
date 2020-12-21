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

package com.smeup.dbnative.jt400

import com.ibm.as400.access.*
import com.smeup.dbnative.ConnectionConfig
import com.smeup.dbnative.DBManagerBaseImpl
import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.model.FileMetadata

open class JT400DBMMAnager(override val connectionConfig: ConnectionConfig) : DBManagerBaseImpl()  {

    private var openedFile = mutableMapOf<String, JT400DBFile>()

    //   as400://SRVLAB01.SMEUP.COM/W_SCAARM
    private val match = Regex("as400://((?:\\w|\\.)+)/(\\w+)").find(connectionConfig.url)
    private val host : String by lazy {
        match!!.destructured.component1()
    }

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

    override fun openFile(name: kotlin.String) : DBFile {
        require(existFile(name)) {
            "Cannot open unregistered file $name"
        }
        //
        if (openedFile.containsKey(name)) {
            return openedFile.getValue(name)
        }
        //
        val fileName = QSYSObjectPathName(library, name, "*FILE", "MBR")
        val path = fileName.path
        println("Path: " + path)
        val file = KeyedFile(connection, path)
        //val rf = AS400FileRecordDescription(system, path).retrieveRecordFormat()
        //file.recordFormat = rf[0]
        file.setRecordFormat() // Loads the record format directly from the server.
        file.open(AS400File.READ_WRITE, 0, AS400File.COMMIT_LOCK_LEVEL_NONE)
        val jt400File = JT400DBFile(name, metadataOf(name), file)
        openedFile.putIfAbsent(name, jt400File)
        return jt400File

    }

    override fun closeFile(name: String) {
        val file = openedFile[name]
        if (file != null) {
            file.close()
            openedFile.remove(name)
        }
    }

    override fun validateConfig() {
    }

    override fun close() {
        connection.disconnectAllServices()
    }

}