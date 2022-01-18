package com.codality.data.tools.parser

import com.codality.data.tools.parser.serialize.Csv
import com.codality.data.tools.parser.serialize.Json
import com.codality.data.tools.parser.serialize.DelimitedReport
import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.JsonElement
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Paths
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class FileParser(fileFormat: FileFormat, config: ParserConfigMessage.ParserConfig) {

    private val LOG: Logger = LoggerFactory.getLogger(FileParser::class.java)

    private val parser = getFileParser(fileFormat, config)

    private fun getFileParser(parserType: FileFormat, config: ParserConfigMessage.ParserConfig): Serializer {
        return when (parserType) {
            FileFormat.CSV -> {
                Csv(config)
            }
            FileFormat.JSON -> {
                Json(config)
            }
            FileFormat.REPORT -> {
                DelimitedReport(config)
            }
        }
    }

    fun parse(file: File): JsonElement {
        val inputStream = BufferedInputStream(FileInputStream(file))
        val result = parser.parse(inputStream)
        inputStream.close()
        return result
    }

    fun findFilesInPath(path: String, extension: String): List<File> {
        val filePath = Paths.get(path).toFile()
        LOG.info("traversing directory: $filePath")
        return if (filePath.isDirectory) {
            filePath.listFiles()?.fold(mutableListOf<File>()) { acc, subPath ->
                LOG.info("checking file: $subPath")
                if (subPath.isDirectory) {
                    acc.addAll(findFilesInPath(subPath.absolutePath, extension))
                } else if (subPath.isFile && (subPath.name.endsWith(extension)))
                    acc.add(subPath)
                acc
            }!!.toList()
        } else if (filePath.isFile)
            listOf(filePath)
        else
            emptyList()
    }

    fun loadFilesList(fileNames: List<String>): List<InputStream> {
        return fileNames.map { file ->
            File(file).inputStream()
        }
    }
}