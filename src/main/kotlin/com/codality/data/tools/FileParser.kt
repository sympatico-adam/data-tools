package com.codality.data.tools

import com.codality.data.tools.proto.ParserConfigMessage
import java.io.File

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

}