package com.codality.data.tools

import com.codality.data.tools.proto.ParserConfigMessage
import java.io.File
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue

interface FileParser {

    companion object {
        enum class Type { CSV, JSON }

        fun getFileParser(fileType: Type, config: ParserConfigMessage.ParserConfig): FileParser {
            return when (fileType) {
                Type.CSV -> {
                    CsvParser(config)
                }
                Type.JSON -> {
                    JsonParser(config)
                }
            }
        }
    }

    fun parse(file: File): Pair<String, ByteArray>

    fun parseToQueue(file: File, queue: ConcurrentLinkedQueue<Pair<String, ByteArray>>)
}