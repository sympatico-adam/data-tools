package com.codality.data.tools.parser

import com.codality.data.tools.proto.ParserConfigMessage
import java.io.File
import java.io.InputStream
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue
import org.slf4j.Logger
import org.slf4j.LoggerFactory

interface FileParser {

    val config: ParserConfigMessage.ParserConfig

    companion object {

        private val LOG: Logger = LoggerFactory.getLogger(FileParser::class.java)

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

        fun findFilesInPath(path: String, extension: String): List<File> {
            val filePath = Paths.get(path).toFile()
            LOG.info("traversing directory: $filePath")
            val files = mutableListOf<File>()
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

    fun parse(file: File, collection: String)

    fun getQueue(): ConcurrentLinkedQueue<Pair<String, ByteArray>>

}