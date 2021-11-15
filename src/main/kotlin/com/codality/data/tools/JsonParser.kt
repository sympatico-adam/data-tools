package com.codality.data.tools

import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.*
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileReader
import java.io.InputStreamReader
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue

class JsonParser(config: ParserConfigMessage.ParserConfig): FileParser {

    private val parseNested = config.format.json.parseNested

    private val LOG: Logger = LoggerFactory.getLogger(JsonParser::class.java)

    override fun parse(file: File): Pair<String, ByteArray> {
        LOG.info("Streaming json file:\n${file.path}\n")
        val json = Utils.deserializeJsonFile(file)
        LOG.info("normalizing json structure:\n$json\n")
        val result = parseJson(json)
        LOG.info(
            "*** finished parsing\n" +
                    "*** ${file.name} " +
                    "***********************" +
                    "\n${result}\n"
        )
        return file.nameWithoutExtension to result.toString().toByteArray()
    }

    override fun parseToQueue(file: File, queue: ConcurrentLinkedQueue<Pair<String, ByteArray>>) {
        TODO("Not yet implemented")
    }

    private fun parseJson(jsonElement: JsonElement): JsonElement {
        return if (parseNested)
            Utils.parseNestedElements(jsonElement)
        else
            Utils.parseElements(jsonElement)

    }
}