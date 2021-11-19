package com.codality.data.tools.parser

import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.lang.Integer.min
import java.util.concurrent.ConcurrentLinkedQueue
import org.slf4j.LoggerFactory

class ReportParser(override val config: ParserConfigMessage.ParserConfig) : FileParser {

    private val parserQueue = ConcurrentLinkedQueue<Pair<String, ByteArray>>()

    private val regex = Regex(config.format.report.delimiterRegex)
    private val lineSeparator = Regex(config.format.report.lineSeparator)

    override fun parse(file: File) {
        LOG.info("Streaming csv file:\n${file.path}\n")
        val inputStream = BufferedInputStream(FileInputStream(file))
        parseReport(file.nameWithoutExtension, inputStream)
        LOG.info("*** finished parsing\n" +
                "*** ${file.name} " +
                "***********************")
    }

    private fun parseReport(collectionName: String, inputStream: InputStream) {
        val reader = inputStream.bufferedReader()
        val lines = reader.readLines()
        val boundaries = splitBoundaries(lines)
        if (boundaries.isNotEmpty())
            boundaries.take(boundaries.size.minus(1)).fold(boundaries[0]) { acc, boundary ->
                val chunk = lines.subList(min(acc.plus(1), boundary), boundary)
                try {
                    val columnsJson = mapColsToJson(chunk)
                    parserQueue.add(collectionName to columnsJson.toString().toByteArray())
                } catch (e: ArrayIndexOutOfBoundsException) {
                    LOG.error("Problem parsing chunk: \n$chunk\n)}")
                    e.printStackTrace()
                }
            boundary
        }
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

    private fun splitBoundaries(lines: List<String>): List<Int> {
        return lines.foldIndexed(mutableListOf()) { idx, acc, line ->
            if (line.matches(lineSeparator))
                acc.add(idx)
            acc
        }
    }

    override fun getQueue(): ConcurrentLinkedQueue<Pair<String, ByteArray>> {
        return parserQueue
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(ReportParser::class.java)
    }

}