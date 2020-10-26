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

import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.nosql.utils.MUNICIPALITY_TABLE_NAME
import com.smeup.dbnative.nosql.utils.createAndPopulateMunicipalityTable
import com.smeup.dbnative.nosql.utils.dbManagerForTest
import com.smeup.dbnative.nosql.utils.getMunicipalityName
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.Ignore
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NoSQLMunicipalityTest {

    private lateinit var dbManager: NoSQLDBMManager

    @Before
    fun initEnv() {
        dbManager = dbManagerForTest()
        createAndPopulateMunicipalityTable(dbManager)
    }

    @Test
    fun t01_notFindErbascoButFindErbuscoWithChain() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult1 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBASCO"))
        assertEquals(0, chainResult1.record.size)
        val chainResult2 = dbFile.chain(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO"))
        assertEquals("ERBUSCO", getMunicipalityName(chainResult2.record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t02_findTwoAfterErbuscoWithSetll4AndReadE3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ERBUSCO", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals("ESINE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals("FIESSE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t02B_findTwoAfterErbuscoWithSetll4AndRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ERBUSCO", getMunicipalityName(dbFile.read().record))
        assertEquals("ESINE", getMunicipalityName(dbFile.read().record))
        assertEquals("FIESSE", getMunicipalityName(dbFile.read().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t03_findTwoBeforeErbuscoWithSetll4AndReadPE3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals(
            "EDOLO",
            getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS")).record)
        )
        assertEquals(
            "DESENZANO DEL GARDA",
            getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS")).record)
        )
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t03B_findTwoBeforeErbuscoWithSetll4AndReadP() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("EDOLO", getMunicipalityName(dbFile.readPrevious().record))
        assertEquals("DESENZANO DEL GARDA", getMunicipalityName(dbFile.readPrevious().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }


    @Test
    fun t04_findLastOfBergamoWithSetll4AndReadPE2() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ACQUAFREDDA")))
        assertEquals(0, dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS")).record.size)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS", "ACQUAFREDDA")))
        assertEquals("ZOGNO", getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t05_findFirstOfComoWithSetgt4AndReadE2() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ZONE")))
        assertEquals(0, dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record.size)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ZONE")))
        assertEquals("ALBAVILLA", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t05B_findFirstOfComoWithSetgt4AndRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ZONE")))
        assertEquals("ALBAVILLA", getMunicipalityName(dbFile.read().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t06_findTwoAfterErbuscoWithSetGt4AndReadE3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ESINE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals("FIESSE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t06B_findTwoAfterErbuscoWithSetgt4AndRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ESINE", getMunicipalityName(dbFile.read().record))
        assertEquals("FIESSE", getMunicipalityName(dbFile.read().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t06C_findTwoBeforeErbuscoWithSetgt4AndReadP() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "ERBUSCO")))
        assertEquals("ERBUSCO", getMunicipalityName(dbFile.readPrevious().record))
        assertEquals("EDOLO", getMunicipalityName(dbFile.readPrevious().record))
        assertEquals("DESENZANO DEL GARDA", getMunicipalityName(dbFile.readPrevious().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t07_findAllAfterVioneWithSetGt4AndReadE3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "LOM", "BS", "VIONE")))
        assertEquals("VISANO", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals("VOBARNO", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals("ZONE", getMunicipalityName(dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record))
        assertEquals(0, dbFile.readEqual(buildMunicipalityKey("IT", "LOM", "BS")).record.size)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t08_findAllOfMateraWithSetll3AndReadE3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val key3 = buildMunicipalityKey("IT", "BAS", "MT")
        assertTrue(dbFile.setll(key3))
        assertEquals("ACCETTURA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("ALIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("BERNALDA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("CALCIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("CIRIGLIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("COLOBRARO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("CRACO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("FERRANDINA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("GARAGUSO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("GORGOGLIONE", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("GRASSANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("GROTTOLE", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("IRSINA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("MATERA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("MIGLIONICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("MONTALBANO JONICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("MONTESCAGLIOSO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("NOVA SIRI", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("OLIVETO LUCANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("PISTICCI", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("POLICORO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("POMARICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("ROTONDELLA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("SALANDRA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("SAN GIORGIO LUCANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("SAN MAURO FORTE", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("SCANZANO JONICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("STIGLIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("TRICARICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("TURSI", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("VALSINNI", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals(0, dbFile.readEqual(key3).record.size)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t08B_findAllOfMateraWithReadE3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val key3 = buildMunicipalityKey("IT", "BAS", "MT")
        assertEquals("ACCETTURA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("ALIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("BERNALDA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("CALCIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("CIRIGLIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("COLOBRARO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("CRACO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("FERRANDINA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("GARAGUSO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("GORGOGLIONE", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("GRASSANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("GROTTOLE", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("IRSINA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("MATERA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("MIGLIONICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("MONTALBANO JONICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("MONTESCAGLIOSO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("NOVA SIRI", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("OLIVETO LUCANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("PISTICCI", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("POLICORO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("POMARICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("ROTONDELLA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("SALANDRA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("SAN GIORGIO LUCANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("SAN MAURO FORTE", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("SCANZANO JONICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("STIGLIANO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("TRICARICO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("TURSI", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("VALSINNI", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals(0, dbFile.readEqual(key3).record.size)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t09_findAllOfMateraAndThenBasilicataWithChain3AndReadE2() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val chainResult = dbFile.chain(buildMunicipalityKey("IT", "BAS", "MT"))
        assertEquals("ACCETTURA", getMunicipalityName(chainResult.record))
        val key2 = buildMunicipalityKey("IT", "BAS")
        assertEquals("ALIANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("BERNALDA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("CALCIANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("CIRIGLIANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("COLOBRARO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("CRACO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("FERRANDINA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("GARAGUSO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("GORGOGLIONE", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("GRASSANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("GROTTOLE", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("IRSINA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("MATERA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("MIGLIONICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("MONTALBANO JONICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("MONTESCAGLIOSO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("NOVA SIRI", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("OLIVETO LUCANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("PISTICCI", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("POLICORO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("POMARICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("ROTONDELLA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("SALANDRA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("SAN GIORGIO LUCANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("SAN MAURO FORTE", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("SCANZANO JONICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("STIGLIANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("TRICARICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("TURSI", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("VALSINNI", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("ABRIOLA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("ACERENZA", getMunicipalityName(dbFile.readEqual(key2).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t09B_findAllOfMateraAndThenBasilicataWithReadE2() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val key2 = buildMunicipalityKey("IT", "BAS")
        assertEquals("ACCETTURA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("ALIANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("BERNALDA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("CALCIANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("CIRIGLIANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("COLOBRARO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("CRACO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("FERRANDINA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("GARAGUSO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("GORGOGLIONE", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("GRASSANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("GROTTOLE", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("IRSINA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("MATERA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("MIGLIONICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("MONTALBANO JONICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("MONTESCAGLIOSO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("NOVA SIRI", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("OLIVETO LUCANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("PISTICCI", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("POLICORO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("POMARICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("ROTONDELLA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("SALANDRA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("SAN GIORGIO LUCANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("SAN MAURO FORTE", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("SCANZANO JONICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("STIGLIANO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("TRICARICO", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("TURSI", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("VALSINNI", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("ABRIOLA", getMunicipalityName(dbFile.readEqual(key2).record))
        assertEquals("ACERENZA", getMunicipalityName(dbFile.readEqual(key2).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t10_findNothingWithSetll3AndReadPE3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        assertEquals(0, dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS")).record.size)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t10B_findNothingWithReadPE3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertEquals(0, dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM", "BS")).record.size)
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t10C_findLastOfBergamoWithSetll3AndReadPE2() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setll(buildMunicipalityKey("IT", "LOM", "BS")))
        assertEquals("ZOGNO", getMunicipalityName(dbFile.readPreviousEqual(buildMunicipalityKey("IT", "LOM")).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    // NB: there is a "IT","BAQ","AA","AAAA" record at the end of the file...
    @Test
    fun t11_findAAAAAfterLastOfAbruzzoWithSetll2AndRead() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        assertTrue(dbFile.setgt(buildMunicipalityKey("IT", "ABR")))
        assertEquals("AAAA", getMunicipalityName(dbFile.read().record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test @Ignore
    fun t12_findLastRecordWithReadInSequentialAccess() {
        // with sequential access (not implemented) read until the eof -> the last record must be "IT","BAQ","AA","AAAA"
    }

    @Test
    fun t13_someReadE3AndReadPE3WithSetll3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val key3 = buildMunicipalityKey("IT", "LOM", "BS")
        assertTrue(dbFile.setll(key3))
        assertEquals("ACQUAFREDDA", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("ADRO", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("AGNOSINE", getMunicipalityName(dbFile.readEqual(key3).record))
        assertEquals("ADRO", getMunicipalityName(dbFile.readPreviousEqual(key3).record))
        assertEquals("ACQUAFREDDA", getMunicipalityName(dbFile.readPreviousEqual(key3).record))
        assertEquals("ADRO", getMunicipalityName(dbFile.readEqual(key3).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }

    @Test
    fun t14_someReadE3AndReadPE3WithSetGt3() {
        val dbFile = dbManager.openFile(MUNICIPALITY_TABLE_NAME)
        val key3A = buildMunicipalityKey("IT", "LOM", "BG")
        assertTrue(dbFile.setgt(key3A))
        val key3B = buildMunicipalityKey("IT", "LOM", "BS")
        assertEquals("ACQUAFREDDA", getMunicipalityName(dbFile.readEqual(key3B).record))
        assertEquals("ADRO", getMunicipalityName(dbFile.readEqual(key3B).record))
        assertEquals("AGNOSINE", getMunicipalityName(dbFile.readEqual(key3B).record))
        assertEquals("ADRO", getMunicipalityName(dbFile.readPreviousEqual(key3B).record))
        assertEquals("ACQUAFREDDA", getMunicipalityName(dbFile.readPreviousEqual(key3B).record))
        assertEquals("ADRO", getMunicipalityName(dbFile.readEqual(key3B).record))
        dbManager.closeFile(MUNICIPALITY_TABLE_NAME)
    }



    @After
    fun destroyEnv() {

    }


    private fun buildMunicipalityKey(vararg values: String): List<RecordField> {
        val recordFields = mutableListOf<RecordField>()
        val keys = arrayOf("NAZ", "REG", "PROV", "CITTA")
        for ((index, value) in values.withIndex()) {
            if (keys.size> index) {
                recordFields.add(RecordField(keys[index], value))
            }
        }
        return recordFields
    }

}

