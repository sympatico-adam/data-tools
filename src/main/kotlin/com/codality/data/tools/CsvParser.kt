package com.codality.data.tools

import com.codality.data.tools.file.CsvLoader
import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.JsonIOException
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import org.slf4j.LoggerFactory
import java.io.*

class CsvParser(pattern: String, private val fields: List<ParserConfigMessage.CsvField>) {

    private val regex = Regex(pattern)

    @Throws(IOException::class)
    fun parse(file: File): Pair<String, ByteArray> {
        LOG.info("Streaming json file:\n${file.path}\n")
        val result = parseCsv(InputStreamReader(BufferedInputStream(FileInputStream(file))))
        LOG.info(
            "*** finished parsing\n" +
                    "*** ${file.name} " +
                    "***********************" +
                    "\n${result}\n"
        )
        return (file.name to result.toByteArray())
    }

    fun parseCsv(reader: InputStreamReader): String {
        var lineCount = 0L
        val stringBuilder = StringBuilder()
        reader.readLines().foldRight(StringBuilder()) { line, acc ->
            try {
                val splitLine = toJson(line)
                acc.append("$splitLine\n".trimIndent())
                lineCount++
            } catch (e: ArrayIndexOutOfBoundsException) {
                // TODO - add metrics counter
                println(line)
                e.printStackTrace()
            }
            acc
        }
        return stringBuilder.toString()
    }

    @Throws(ArrayIndexOutOfBoundsException::class)
    private fun toJson(line: CharSequence): String {
        val splitLine = regex.split(normalize(line))
        val json = JsonObject()
        fields.forEach { field ->
            try {
                json.add(field.name, JsonPrimitive(splitLine[field.sourceColumn]))
            } catch (e: Exception) {
                LOG.error("Failed to parse field: ${field.name}:${field.sourceColumn} - \n${splitLine.joinToString("\n")}")
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
        private val LOG = LoggerFactory.getLogger(CsvLoader::class.java)
    }
}