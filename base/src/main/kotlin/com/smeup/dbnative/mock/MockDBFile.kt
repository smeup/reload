package com.smeup.dbnative.mock

import com.smeup.dbnative.file.DBFile
import com.smeup.dbnative.file.Record
import com.smeup.dbnative.file.RecordField
import com.smeup.dbnative.file.Result
import com.smeup.dbnative.log.Logger
import com.smeup.dbnative.model.FileMetadata
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Mock implementation of DBFile.
 * All methods are not implemented.
 */
class MockDBFile(override var name: String, override var fileMetadata: FileMetadata, override var logger: Logger?) :
    DBFile {

    val maxMockRecords = 522132
    var returnedRecords = 0
    lateinit var mockedResult: Result


    private fun nextIteration(): Result {

        var allFields = when (fileMetadata.tableName) {
            "BRENTI0F" -> getBRENTI()
            "BRCOMM0F" -> getBRCOMM()
            "MU24020F" -> getMU24020()
            "MU24010F" -> getMU24010()
            "MU24000F" -> getMU24000()
            else -> {
                fileMetadata.fields.map { field ->
                    RecordField(field.name, if (field.numeric) "0" else if (field.text.isBlank())  field.name else field.text)
                }.toList()
            }
        }
        if (returnedRecords == 522131 ){
            var dataField = allFields.find { rField -> rField.name == "M240DATA" }


            val previousData = dataField?.value

            if (previousData != null) {
                val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
                val previousDate = LocalDate.parse(previousData, formatter)
                val nextDate = previousDate.plusDays(1)
                allFields = allFields.filter { recordField -> recordField.name != "M240DATA" }
                allFields = allFields.plus(RecordField("M240DATA", nextDate.format(formatter)))
            }
        }
        val record = Record(*allFields.toTypedArray())

        mockedResult = Result(record = record)

        returnedRecords++

        return mockedResult
    }

    private fun getMU24000(): MutableList<RecordField> {
        return mutableListOf(

            RecordField("M240PG", "MU_240_J2 "),
            RecordField("M240Z1", "2024-05-17T15:10:33.435Z  "),
            RecordField("M240Z2", "2024-05-17T15:29:44.994Z  "),
            RecordField("M240DE", "1133407"),
            RecordField("M240SY", "KOKOS     "),
            RecordField("M240NR", "522131"),
            RecordField("M240NT", "Start date 00000000 end date 99999999")
        )
    }

    private fun getMU24010(): MutableList<RecordField> {
        return mutableListOf(
            RecordField("M240AN", "2022"),
            RecordField("M240CL", "CALPAO         "),
            RecordField("M240TC", "P00            "),
            RecordField("M240AZ", "10             "),
            RecordField("M240T1", "0.000000"),
            RecordField("M240T2", "0.000000"),
            RecordField("M240T3", "0.000000"),
            RecordField("M240TS", "2024-10-16-15.15.18.675756")
        )
    }

    private fun getMU24020(): MutableList<RecordField> {
        return mutableListOf(
            RecordField("M240IDOJ", "0002975404"),
            RecordField("M240ATV0", "3"),
            RecordField("M240DATA", "20230926"),
            RecordField("M240NOME", "BANSTE         "),
            RecordField("M240CDC", "IC-FORMAZ      "),
            RecordField("M240TIPO", "   "),
            RecordField("M240ORE", "1.50"),
            RecordField("M240VIAC", "0.00"),
            RecordField("M240VIAS", "8.10"),
            RecordField("M240KM", "0"),
            RecordField("M240KMSE", "0"),
            RecordField("M240PEDC", "0.00"),
            RecordField("M240PEDS", "0.00"),
            RecordField("M240RISC", "0.00"),
            RecordField("M240RISS", "0.00"),
            RecordField("M240ALBC", "0.00"),
            RecordField("M240ALBS", "0.00"),
            RecordField("M240SPEC", "0.00"),
            RecordField("M240SPES", "0.00"),
            RecordField("M240CAUS", "   "),
            RecordField("M240TCOM", " "),
            RecordField(
                "M240COM1",
                "                                                                                                                                                                                                                  "
            ),
            RecordField(
                "M240COM2",
                "                                                                                                                                                                                                                  "
            ),
            RecordField("M240DIAR", "0.00"),
            RecordField("M240DIUS", "0.00"),
            RecordField("M240NFAT", "          "),
            RecordField("M240DFAT", "0"),
            RecordField("M240COD1", "               "),
            RecordField("M240COD2", "               "),
            RecordField("M240COD3", "               "),
            RecordField("M240COD4", "               "),
            RecordField("M240COD5", "               "),
            RecordField("M240COD6", "BRP            "),
            RecordField("M240COD7", "               "),
            RecordField("M240COD8", "               "),
            RecordField("M240COD9", "11             "),
            RecordField("M240COD0", "389859         "),
            RecordField("M240NUM1", "0.000000"),
            RecordField("M240NUM2", "0.000000"),
            RecordField("M240NUM3", "0.000000"),
            RecordField("M240NUM4", "0.000000"),
            RecordField("M240NUM5", "0.000000"),
            RecordField("M240FL01", " "),
            RecordField("M240FL02", " "),
            RecordField("M240FL03", " "),
            RecordField("M240FL04", " "),
            RecordField("M240FL05", " "),
            RecordField("M240FL06", " "),
            RecordField("M240FL07", " "),
            RecordField("M240FL08", "9"),
            RecordField("M240FL09", " "),
            RecordField("M240FL10", "1"),
            RecordField("M240FL11", " "),
            RecordField("M240FL12", " "),
            RecordField("M240FL13", " "),
            RecordField("M240FL14", " "),
            RecordField("M240FL15", " "),
            RecordField("M240FL16", " "),
            RecordField("M240FL17", " "),
            RecordField("M240FL18", " "),
            RecordField("M240FL19", " "),
            RecordField("M240FL20", " "),
            RecordField("M240USIN", "BANSTE    "),
            RecordField("M240DTIN", "20231001"),
            RecordField("M240ORIN", "113606"),
            RecordField("M240USAG", "PIZBAR    "),
            RecordField("M240DTAG", "20231003"),
            RecordField("M240ORAG", "152709")

        )
    }

    private fun getBRENTI(): MutableList<RecordField> {
        return mutableListOf(
            RecordField("E§TRAG", "COL"),
            RecordField("E§CRAG", "BERSTE         "),
            RecordField("E§SCEN", "               "),
            RecordField("E§AZIE", "  "),
            RecordField("E§IDOJ", "1600467384"),
            RecordField("E§DINV", "0"),
            RecordField("E§DFNV", "99991231"),
            RecordField("E§GRUP", "COL"),
            RecordField("E§NMNE", "                                   "),
            RecordField("E§NCOD", "               "),
            RecordField("E§LIVE", "2"),
            RecordField("E§STAT", "10"),
            RecordField("E§RAGS", "BERNABEI STEFANO                   "),
            RecordField("E§RAGA", "                                   "),
            RecordField("E§INDI", "VIA TRASIMENO, 48                  "),
            RecordField("E§INDA", "                                   "),
            RecordField("E§TELE", "                    "),
            RecordField("E§TFAX", "                    "),
            RecordField("E§TELX", "                    "),
            RecordField(
                "E§IEMA",
                "                                                                                                                                    "
            ),
            RecordField("E§PECO", "                    "),
            RecordField("E§CNAZ", "IT    "),
            RecordField("E§CREG", "03        "),
            RecordField("E§CCOM", "F205      "),
            RecordField("E§LOCA", "MILANO                             "),
            RecordField("E§PROV", "MI        "),
            RecordField("E§NAZI", "                         "),
            RecordField("E§CAPA", "20128     "),
            RecordField("E§ZONA", "   "),
            RecordField("E§LING", "   "),
            RecordField("E§VALU", "    "),
            RecordField("E§TSOG", "F  "),
            RecordField("E§CPAI", "                              "),
            RecordField("E§COFI", "BRNSFN93S19F205G    "),
            RecordField("E§CPAE", "                              "),
            RecordField("E§CTA1", "      "),
            RecordField("E§CTA2", "      "),
            RecordField("E§CTA3", "      "),
            RecordField("E§DVES", "0"),
            RecordField("E§CDIN", "               "),
            RecordField("E§DDIN", "0"),
            RecordField("E§TNOM", "NOM"),
            RecordField("E§CNOM", "047293         "),
            RecordField("E§TSPE", "   "),
            RecordField("E§CSPE", "               "),
            RecordField("E§TCON", "FOR"),
            RecordField("E§CCON", "               "),
            RecordField("E§TINC", "AZI"),
            RecordField("E§CINC", "10             "),
            RecordField("E§TPRZ", "   "),
            RecordField("E§CPRZ", "               "),
            RecordField("E§TCRR", "CLI"),
            RecordField("E§CCRR", "               "),
            RecordField("E§TVET", "   "),
            RecordField("E§CVET", "               "),
            RecordField("E§CACO", "3602           "),
            RecordField("E§CACR", "   "),
            RecordField("E§CLAB", "    "),
            RecordField("E§CLAV", "    "),
            RecordField("E§CSME", "62    "),
            RecordField("E§CONS", "   "),
            RecordField("E§SPED", "   "),
            RecordField("E§IMBA", "   "),
            RecordField("E§CAUS", "     "),
            RecordField("E§GCHI", "  "),
            RecordField("E§GCON", "  "),
            RecordField("E§GCOL", "  "),
            RecordField("E§AGEN", "   "),
            RecordField("E§PERP", "0.000"),
            RecordField("E§AGE1", "   "),
            RecordField("E§PER1", "0.000"),
            RecordField("E§CRCP", "     "),
            RecordField("E§COPA", "211"),
            RecordField("E§CSP1", "   "),
            RecordField("E§CSP2", "   "),
            RecordField("E§CSP3", "   "),
            RecordField("E§CSP4", "   "),
            RecordField("E§CSP5", "   "),
            RecordField("E§LIST", "   "),
            RecordField("E§SCON", "0.00"),
            RecordField("E§PRIO", "  "),
            RecordField("E§FIDO", "0.000000"),
            RecordField("E§BANC", "0306932430     "),
            RecordField("E§CCOR", "100000006343        "),
            RecordField("E§CBA2", "               "),
            RecordField("E§IBAN", "IT95G0306932430100000006343        "),
            RecordField("E§SWIF", "               "),
            RecordField("E§CBA3", "               "),
            RecordField("E§DTIE", "0"),
            RecordField("E§DT01", "0"),
            RecordField("E§DT02", "0"),
            RecordField("E§DT03", "0"),
            RecordField("E§DT04", "0"),
            RecordField("E§DT05", "0"),
            RecordField("E§DT06", "0"),
            RecordField("E§DT07", "0"),
            RecordField("E§DT08", "0"),
            RecordField("E§DT09", "0"),
            RecordField("E§DT10", "0"),
            RecordField("E§COD1", "               "),
            RecordField("E§COD2", "               "),
            RecordField("E§COD3", "85             "),
            RecordField("E§COD4", "               "),
            RecordField("E§COD5", "               "),
            RecordField("E§COD6", "               "),
            RecordField("E§COD7", "               "),
            RecordField("E§COD8", "               "),
            RecordField("E§COD9", "               "),
            RecordField("E§COD0", "               "),
            RecordField("E§NUM1", "0.00000"),
            RecordField("E§NUM2", "0.00000"),
            RecordField("E§NUM3", "0.00000"),
            RecordField("E§NUM4", "0.00000"),
            RecordField("E§NUM5", "0.00000"),
            RecordField("E§NUM6", "0.00000"),
            RecordField("E§NUM7", "0.00000"),
            RecordField("E§NUM8", "0.00000"),
            RecordField("E§NUM9", "0.00000"),
            RecordField("E§NUM0", "0.00000"),
            RecordField("E§FL01", " "),
            RecordField("E§FL02", " "),
            RecordField("E§FL03", " "),
            RecordField("E§FL04", " "),
            RecordField("E§FL05", " "),
            RecordField("E§FL06", " "),
            RecordField("E§FL07", " "),
            RecordField("E§FL08", " "),
            RecordField("E§FL09", " "),
            RecordField("E§FL10", " "),
            RecordField("E§FL11", " "),
            RecordField("E§FL12", " "),
            RecordField("E§FL13", " "),
            RecordField("E§FL14", " "),
            RecordField("E§FL15", " "),
            RecordField("E§FL16", " "),
            RecordField("E§FL17", " "),
            RecordField("E§FL18", " "),
            RecordField("E§FL19", " "),
            RecordField("E§FL20", " "),
            RecordField("E§FL21", " "),
            RecordField("E§FL22", " "),
            RecordField("E§FL23", "2"),
            RecordField("E§FL24", " "),
            RecordField("E§FL25", " "),
            RecordField("E§FL26", " "),
            RecordField("E§FL27", " "),
            RecordField("E§FL28", " "),
            RecordField("E§FL29", " "),
            RecordField("E§FL30", " "),
            RecordField("E§FL31", " "),
            RecordField("E§FL32", " "),
            RecordField("E§FL33", " "),
            RecordField("E§FL34", " "),
            RecordField("E§FL35", " "),
            RecordField("E§FL36", " "),
            RecordField("E§FL37", " "),
            RecordField("E§FL38", " "),
            RecordField("E§FL39", " "),
            RecordField("E§FL40", " "),
            RecordField("E§ORIN", "155019"),
            RecordField("E§DTIN", "20210125"),
            RecordField("E§USIN", "MAZLAU    "),
            RecordField("E§ORAG", "122144"),
            RecordField("E§DTAG", "20230314"),
            RecordField("E§USAG", "COLIRE    ")
        )
    }


    private fun getBRCOMM(): MutableList<RecordField> {
        return mutableListOf(
            RecordField("M\$COMM", "MANA121276"),
            RecordField("M\$DECM", "Centro JOBS R18                    "),
            RecordField("M\$LIVE", "9"),
            RecordField("M\$SSST", "XE"),
            RecordField("M\$STAT", "90"),
            RecordField("M\$TICM", "99 "),
            RecordField("M\$NACM", "SV"),
            RecordField("M\$COMR", "MANA121276"),
            RecordField("M\$SSTC", "  "),
            RecordField("M\$STCM", "     "),
            RecordField("M\$CDMG", "01 "),
            RecordField("M\$PRIO", "  "),
            RecordField("M\$TIRF", "CN"),
            RecordField("M\$PAOG", "AZI       "),
            RecordField("M\$CDOG", "10             "),
            RecordField("M\$DSOG", "SME UP SPA                         "),
            RecordField("M\$DSAG", "                                   "),
            RecordField("M\$TIRE", "CN"),
            RecordField("M\$PARE", "COL       "),
            RecordField("M\$CDRE", "BONVIT         "),
            RecordField("M\$TICG", "  "),
            RecordField("M\$PACG", "          "),
            RecordField("M\$CDCG", "               "),
            RecordField("M\$TIEN", "CLI"),
            RecordField("M\$COEN", "200633         "),
            RecordField("M\$TIE2", "   "),
            RecordField("M\$COE2", "               "),
            RecordField("M\$TIE3", "   "),
            RecordField("M\$COE3", "               "),
            RecordField("M\$RFES", "                              "),
            RecordField("M\$DT01", "0"),
            RecordField("M\$DT02", "0"),
            RecordField("M\$DT03", "0"),
            RecordField("M\$DT04", "0"),
            RecordField("M\$DT05", "0"),
            RecordField("M\$DT06", "0"),
            RecordField("M\$DT07", "0"),
            RecordField("M\$DT08", "0"),
            RecordField("M\$DT09", "0"),
            RecordField("M\$DT10", "0"),
            RecordField("M\$QI01", "500.00000"),
            RecordField("M\$QI02", "500.00000"),
            RecordField("M\$QI03", "0.00000"),
            RecordField("M\$QI04", "0.00000"),
            RecordField("M\$QI05", "0.00000"),
            RecordField("M\$QI06", "0.00000"),
            RecordField("M\$QI07", "0.00000"),
            RecordField("M\$QI08", "0.00000"),
            RecordField("M\$QI09", "0.00000"),
            RecordField("M\$QI10", "0.00000"),
            RecordField("M\$TOG1", "            "),
            RecordField("M\$COG1", "               "),
            RecordField("M\$TOG2", "            "),
            RecordField("M\$COG2", "               "),
            RecordField("M\$TOG3", "            "),
            RecordField("M\$COG3", "               "),
            RecordField("M\$TOG4", "            "),
            RecordField("M\$COG4", "               "),
            RecordField("M\$TOG5", "            "),
            RecordField("M\$COG5", "               "),
            RecordField("M\$TOG6", "            "),
            RecordField("M\$COG6", "               "),
            RecordField("M\$COD1", "               "),
            RecordField("M\$COD2", "               "),
            RecordField("M\$COD3", "               "),
            RecordField("M\$COD4", "I007           "),
            RecordField("M\$COD5", "               "),
            RecordField("M\$CD06", "               "),
            RecordField("M\$CD07", "               "),
            RecordField("M\$CD08", "               "),
            RecordField("M\$CD09", "               "),
            RecordField("M\$CD10", "               "),
            RecordField("M\$NUM1", "0.00000"),
            RecordField("M\$NUM2", "0.00000"),
            RecordField("M\$NUM3", "0.00000"),
            RecordField("M\$NUM4", "0.00000"),
            RecordField("M\$NUM5", "0.00000"),
            RecordField("M\$NR06", "0.00000"),
            RecordField("M\$NR07", "0.00000"),
            RecordField("M\$NR08", "0.00000"),
            RecordField("M\$NR09", "0.00000"),
            RecordField("M\$NR10", "0.00000"),
            RecordField("M\$FL01", "9"),
            RecordField("M\$FL02", "1"),
            RecordField("M\$FL03", " "),
            RecordField("M\$FL04", " "),
            RecordField("M\$FL05", " "),
            RecordField("M\$FL06", " "),
            RecordField("M\$FL07", " "),
            RecordField("M\$FL08", " "),
            RecordField("M\$FL09", " "),
            RecordField("M\$FL10", " "),
            RecordField("M\$FL11", " "),
            RecordField("M\$FL12", " "),
            RecordField("M\$FL13", " "),
            RecordField("M\$FL14", " "),
            RecordField("M\$FL15", " "),
            RecordField("M\$FL16", " "),
            RecordField("M\$FL17", " "),
            RecordField("M\$FL18", " "),
            RecordField("M\$FL19", " "),
            RecordField("M\$FL20", "2"),
            RecordField("M\$FL21", " "),
            RecordField("M\$FL22", " "),
            RecordField("M\$FL23", " "),
            RecordField("M\$FL24", " "),
            RecordField("M\$FL25", " "),
            RecordField("M\$FL26", " "),
            RecordField("M\$FL27", " "),
            RecordField("M\$FL28", " "),
            RecordField("M\$FL29", " "),
            RecordField("M\$FL30", " "),
            RecordField("M\$FL31", " "),
            RecordField("M\$FL32", " "),
            RecordField("M\$FL33", " "),
            RecordField("M\$FL34", " "),
            RecordField("M\$FL35", " "),
            RecordField("M\$FL36", " "),
            RecordField("M\$FL37", " "),
            RecordField("M\$FL38", " "),
            RecordField("M\$FL39", " "),
            RecordField("M\$FL40", " "),
            RecordField("M\$DTIN", "20120910"),
            RecordField("M\$ORIN", "0"),
            RecordField("M\$OGIN", "          "),
            RecordField("M\$USIN", "          "),
            RecordField("M\$DTAG", "20120918"),
            RecordField("M\$ORAG", "0"),
            RecordField("M\$OGAG", "          "),
            RecordField("M\$USAG", "BV        ")
        )
    }

    override fun eof(): Boolean {
        return returnedRecords >= maxMockRecords
    }

    override fun equal(): Boolean {
        return true
    }

    override fun setll(key: String): Boolean {
        return true
    }

    override fun setll(keys: List<String>): Boolean {
        return true
    }

    override fun setgt(key: String): Boolean {
        return true
    }

    override fun setgt(keys: List<String>): Boolean {
        return true
    }

    override fun chain(key: String): Result {
        return nextIteration()
    }

    override fun chain(keys: List<String>): Result {
        return nextIteration()
    }

    override fun read(): Result {
        return nextIteration()

    }

    override fun readPrevious(): Result {
        return nextIteration()

    }

    override fun readEqual(): Result {
        return nextIteration()

    }

    override fun readEqual(key: String): Result {
        return nextIteration()

    }

    override fun readEqual(keys: List<String>): Result {
        return nextIteration()

    }

    override fun readPreviousEqual(): Result {
        return nextIteration()

    }

    override fun readPreviousEqual(key: String): Result {
        return nextIteration()

    }

    override fun readPreviousEqual(keys: List<String>): Result {
        return nextIteration()

    }

    override fun write(record: Record): Result {
        return mockedResult
    }

    override fun update(record: Record): Result {
        return mockedResult
    }

    override fun delete(record: Record): Result {
        return mockedResult
    }
}
