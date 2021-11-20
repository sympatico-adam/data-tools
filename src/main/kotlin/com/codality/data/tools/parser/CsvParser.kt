package com.codality.data.tools.parser

import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import io.netty.util.internal.StringUtil
import org.slf4j.LoggerFactory
import java.io.*
import java.lang.IndexOutOfBoundsException
import java.nio.charset.CharsetDecoder
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.text.Charsets.UTF_16
import kotlin.text.Charsets.UTF_8
import org.yaml.snakeyaml.reader.UnicodeReader

class CsvParser(override val config: ParserConfigMessage.ParserConfig): FileParser {

    private val parserQueue = ConcurrentLinkedQueue<Pair<String, ByteArray>>()
    private val regex = Regex(config.format.csv.delimiterRegex)
    private lateinit var fields: Map<String, Int>
    private val hasHeader = config.format.csv.hasHeader

    @Throws(IOException::class)
    override fun parse(file: File, collection: String) {
        LOG.info("Streaming csv file:\n${file.path}\n")
        val inputStream = BufferedInputStream(FileInputStream(file))
        parseCsv(collection, inputStream)
        LOG.info("*** finished parsing\n" +
                    "*** ${file.name} " +
                    "***********************")
    }

    override fun getQueue(): ConcurrentLinkedQueue<Pair<String, ByteArray>> {
        return parserQueue
    }

    fun parse(collectionName: String, csv: String) {
        val inputStream = ByteArrayInputStream(csv.toByteArray())
        parseCsv(collectionName, inputStream)
    }

    private fun parseFields(headerLine: String?) {
        fields = if (config.format.csv.fieldsList.isNotEmpty()) {
            config.format.csv.fieldsList.associate {
                it.name to it.sourceColumn
            }
        } else if (hasHeader) {
            if (headerLine != null) {
                val headerColumns = regex.split(headerLine)
                headerColumns.mapIndexed { idx, header ->
                    header to idx
                }.toMap()
            } else
                return
        } else
            emptyMap()
    }

    private fun parseCsv(collection: String, inputStream: InputStream) {
        val reader = UnicodeReader(inputStream).buffered()
        val lines = reader.readLines()
        parseFields(lines.first())
        lines.forEach { line ->
            if (line.isNotBlank()) {
                val columns = regex.split(normalize(line))
                if (!columns.containsAll(fields.keys)) {
                    try {
                        val columnsJson = mapColsToJson(columns)
                        parserQueue.add(collection to columnsJson.asJsonObject.toString().toByteArray(UTF_8))
                    } catch (e: ArrayIndexOutOfBoundsException) {
                        LOG.error("Problem parsing line: $line\nsplit lines: ${columns.joinToString("\n")}")
                        e.printStackTrace()
                    }
                }
            }
        }
    }

    @Throws(ArrayIndexOutOfBoundsException::class)
    private fun mapColsToJson(columns: List<String>): JsonObject {
        val json = JsonObject()
        if (fields.isNotEmpty() && columns.size >= fields.size) {
            run fieldLoop@{
                fields.forEach { field ->
                    try {
                        json.add(field.key, JsonPrimitive(columns[field.value]))
                    } catch (e: IndexOutOfBoundsException) {
                        LOG.debug(
                            "Line contains wrong number of columns: ${field.key}:${field.value} - \n${
                                columns.joinToString(
                                    "\n"
                                )
                            }"
                        )
                        e.printStackTrace()
                        return@fieldLoop
                    } catch (e: Exception) {
                        LOG.error("Failed to parse field: ${field.key}:${field.value} - \n${columns.joinToString("\n")}")
                        e.printStackTrace()
                    }
                }
            }
        } else {
            columns.forEachIndexed { idx, line ->
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