package com.codality.data.tools.file

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileDescriptor
import java.io.InputStream
import java.nio.file.Paths
import java.util.*

interface FileLoader {
    companion object {

        private val LOG: Logger = LoggerFactory.getLogger(FileLoader::class.java)

        enum class Type {
            CSV,
            JSON
        }

        val CSV_REGEX_PROPERTY = "csv.regex"
        val CSV_REGEX_DEFAULT = ","
        val CSV_FIELDS_PROPERTY = "csv.fields"


        fun findFilesInPath(path: String, extension: String): List<File> {
            val filePath = Paths.get(path).toFile()
            LOG.info("traversing directory: $filePath")
            val files = mutableListOf<File>()
            filePath.listFiles()!!.forEach { subPath ->
                LOG.info("checking file: $subPath")
                if (subPath.isDirectory) {
                    files.addAll(findFilesInPath(subPath.absolutePath, extension))
                } else if (subPath.isFile && (subPath.name.endsWith(extension)))
                    files.add(subPath)
            }
            return files
        }

        fun getFileLoader(fileType: Type, config: Properties): FileLoader {
            return when (fileType) {
                Type.CSV -> {
                    CsvLoader(config)
                }
                Type.JSON -> {
                    JsonLoader(config)
                }
            }
        }
    }

    val config: Properties

    fun loadFilesFromList(files: List<File>): List<FileDescriptor>

    fun loadFilesInputStreams(files: List<File>): List<InputStream>

}