package org.sympatico.data.client

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Paths

class FileLoader {
    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(FileLoader::class.java)
        fun loadFilesFromPath(path: String, extension: String): List<File> {
            val filePath = Paths.get(path).toFile()
            LOG.info("traversing directory: $filePath")
            val files = mutableListOf<File>()
            filePath.listFiles()!!.forEach { subPath ->
                LOG.info("checking file: $subPath")
                if (subPath.isDirectory) {
                    files.addAll(loadFilesFromPath(subPath.absolutePath, extension))
                } else if (subPath.isFile && (subPath.name.endsWith(extension)))
                    files.add(subPath)
            }
            return files
        }
    }
}