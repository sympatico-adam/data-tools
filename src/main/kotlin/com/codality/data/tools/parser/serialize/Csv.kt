package com.codality.data.tools.parser.serialize

import com.codality.data.tools.parser.Serializer
import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import org.slf4j.LoggerFactory
import java.io.*
import java.lang.IndexOutOfBoundsException

class Csv(val config: ParserConfigMessage.ParserConfig): Serializer {

    private val regex = Regex(config.format.csv.delimiterRegex)
    private lateinit var fields: Map<String, Int>
    private val hasHeader = config.format.csv.hasHeader

    @Throws(IOException::class)
    override fun parse(inputStream: InputStream): JsonElement {
        val reader = inputStream.bufferedReader()
        val result = reader.useLines { lines ->
            lines.foldIndexed(JsonArray()) { idx, acc, line ->
                if (idx == 0)
                    parseFields(line)
                if (line.isNotBlank()) {
                    val columns = regex.split(normalize(line))
                    if (!columns.containsAll(fields.keys)) {
                        try {
                            val columnsJson = mapColsToJson(columns)
                            acc.add(columnsJson.asJsonObject)
                        } catch (e: ArrayIndexOutOfBoundsException) {
                            LOG.error("Problem parsing line: $line\nsplit lines: ${columns.joinToString("\n")}")
                            e.printStackTrace()
                        }
                    }
                }
                acc
            }
        }
        return result
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
        private val LOG = LoggerFactory.getLogger(Csv::class.java)
    }
}