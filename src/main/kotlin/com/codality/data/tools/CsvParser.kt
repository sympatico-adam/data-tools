package com.codality.data.tools

import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import org.slf4j.LoggerFactory
import java.io.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue

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
        return (file.name to result.asJsonArray.toString().toByteArray())
    }

    override fun parseToQueue(file: File, queue: ConcurrentLinkedQueue<Pair<String, ByteArray>>) {
        parseCsv(InputStreamReader(BufferedInputStream(FileInputStream(file)))).asJsonArray.toList()
            .stream().forEach { line ->
                queue.add(file.nameWithoutExtension to line.toString().toByteArray())
            }
    }

    fun parse(csv: String): JsonArray {
        return parseCsv(InputStreamReader(ByteArrayInputStream(csv.toByteArray())))
    }

    private fun parseCsv(reader: InputStreamReader): JsonArray {
        return reader.readLines().foldRight(JsonArray()) { line, acc ->
            val lines = regex.split(normalize(line))
            try {
                val splitLine = mapFieldsToJson(lines)
                acc.add(splitLine)
            } catch (e: ArrayIndexOutOfBoundsException) {
                LOG.error("Problem parsing line: $line\nsplit lines: ${lines.joinToString("\n")}")
                e.printStackTrace()
            }
            acc
        }
    }

    @Throws(ArrayIndexOutOfBoundsException::class)
    private fun mapFieldsToJson(lines: List<String>): JsonObject {
        val json = JsonObject()
        if (fields.isNotEmpty()) {
            fields.forEach { field ->
                try {
                    json.add(field.name, JsonPrimitive(lines[field.sourceColumn]))
                } catch (e: Exception) {
                    LOG.error("Failed to parse field: ${field.name}:${field.sourceColumn} - \n${lines.joinToString("\n")}")
                    e.printStackTrace()
                }
            }
        } else {
            lines.forEachIndexed { idx, line ->
                json.add(idx.toString(), JsonPrimitive(line))
            }
        }
        return json
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