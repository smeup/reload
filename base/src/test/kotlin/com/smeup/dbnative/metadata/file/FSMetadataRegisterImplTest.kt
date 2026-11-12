package com.smeup.dbnative.metadata.file

import com.smeup.dbnative.model.Field
import com.smeup.dbnative.model.FileMetadata
import org.junit.After
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class FSMetadataRegisterImplTest {

    private val name = "TSTFSREGCACHE"

    private fun metadata(text: String) =
        FileMetadata(name, "", listOf(Field("F1", text), Field("F2", numeric = true)), listOf("F1"))

    @After
    fun cleanUp() {
        FSMetadataRegisterImpl.remove(name)
    }

    @Test
    fun registeredMetadataIsRecognizedUntilItChanges() {
        val first = metadata("first")
        FSMetadataRegisterImpl.registerMetadata(first, true)
        assertTrue(FSMetadataRegisterImpl.isRegistered(first))
        assertTrue(FSMetadataRegisterImpl.isRegistered(metadata("first")))
        assertFalse(FSMetadataRegisterImpl.isRegistered(metadata("second")))

        FSMetadataRegisterImpl.registerMetadata(metadata("second"), true)
        assertFalse(FSMetadataRegisterImpl.isRegistered(first))
        assertEquals("second", FSMetadataRegisterImpl.getMetadata(name).fields[0].text)
    }

    @Test
    fun getMetadataReturnsWhatTheJsonFileGivesAndCopies() {
        FSMetadataRegisterImpl.registerMetadata(metadata("x"), true)

        val read = FSMetadataRegisterImpl.getMetadata(name)
        // jsonToMetadata fills an empty tableName with the file name
        assertEquals(name, read.tableName)
        read.tableName = "CHANGED"
        assertNotEquals("CHANGED", FSMetadataRegisterImpl.getMetadata(name).tableName)
    }

    @Test
    fun removedMetadataIsNoLongerRegistered() {
        val m = metadata("x")
        FSMetadataRegisterImpl.registerMetadata(m, true)
        FSMetadataRegisterImpl.remove(name)
        assertFalse(FSMetadataRegisterImpl.contains(name))
        assertFalse(FSMetadataRegisterImpl.isRegistered(m))
    }
}
