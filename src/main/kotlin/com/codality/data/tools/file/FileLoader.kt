package com.codality.data.tools.file

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileDescriptor
import java.io.InputStream
import java.nio.file.Paths
import java.util.*

object FileLoader {

    private val LOG: Logger = LoggerFactory.getLogger(FileLoader::class.java)

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