package com.codality.data.tools.parser.serialize

import com.codality.data.tools.Utils.marshallJsonSequenceToJsonArray
import com.codality.data.tools.parser.Serializer
import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import java.io.InputStream
import org.slf4j.LoggerFactory

class DelimitedReport(val config: ParserConfigMessage.ParserConfig) : Serializer {

    private val regex = Regex(config.format.report.delimiterRegex)
    private val lineSeparator = Regex(config.format.report.lineSeparator)

    override fun parse(inputStream: InputStream): JsonElement {
        val reader = inputStream.bufferedReader()
        val lines = reader.readLines()
        return marshallJsonSequenceToJsonArray(splitBoundaries(lines))
    }

    private fun mapColsToJson(lines: List<String>): JsonObject {
        val json = JsonObject()
        lines.forEach { line ->
        val columns = regex.split(line)
        if (columns.size > 1)
            json.add(columns[0], JsonPrimitive(columns[1]))
        }
        return json
    }

    private fun splitBoundaries(lines: List<String>): Sequence<JsonObject> {
        return lines.foldIndexed<String, MutableList<Int>>(mutableListOf()) { idx, acc, line ->
            if (line.matches(lineSeparator))
                acc.add(idx)
            acc
        }.zipWithNext { start, end ->
            val chunk = lines.subList(start, end)
            mapColsToJson(chunk)
        }.asSequence()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(DelimitedReport::class.java)
    }

}