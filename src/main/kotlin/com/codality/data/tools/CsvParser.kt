package com.codality.data.tools

import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import org.slf4j.LoggerFactory
import java.io.*

class CsvParser(config: ParserConfigMessage.ParserConfig): FileParser {

    private val regex = Regex(config.format.csv.regex)
    private val fields = config.format.csv.fieldsList

    @Throws(IOException::class)
    override fun parse(file: File): Pair<String, ByteArray> {
        LOG.info("Streaming json file:\n${file.path}\n")
        val result = parseCsv(InputStreamReader(BufferedInputStream(FileInputStream(file))))
        LOG.info("*** finished parsing\n" +
                    "*** ${file.name} " +
                    "***********************")
        return (file.name to result.toByteArray())
    }

    private fun parseCsv(reader: InputStreamReader): String {
        var lineCount = 0L
        val stringBuilder = StringBuilder()
        reader.readLines().foldRight(StringBuilder()) { line, acc ->
            val lines = regex.split(normalize(line))
            try {
                val splitLine = mapFieldsToJson(lines)
                acc.append("$splitLine\n".trimIndent())
                lineCount++
            } catch (e: ArrayIndexOutOfBoundsException) {
                LOG.error("Problem parsing line: $line\nsplit lines: ${lines.joinToString("\n")}")
                e.printStackTrace()
            }
            acc
        }
        return stringBuilder.toString()
    }

    @Throws(ArrayIndexOutOfBoundsException::class)
    private fun mapFieldsToJson(lines: List<String>): String {
        val json = JsonObject()
        fields.forEach { field ->
            try {
                json.add(field.name, JsonPrimitive(lines[field.sourceColumn]))
            } catch (e: Exception) {
                LOG.error("Failed to parse field: ${field.name}:${field.sourceColumn} - \n${lines.joinToString("\n")}")
                e.printStackTrace()
            }
        }
        return json.toString()
    }

    private fun normalize(line: CharSequence): String {
        return line.toString()
            .replace("\r\n", "\n")
            .replace("\r", "\n")
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(CsvParser::class.java)
    }
}