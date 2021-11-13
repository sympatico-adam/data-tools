package com.codality.data.tools.file

import com.codality.data.tools.file.FileLoader
import java.io.File
import java.io.FileDescriptor
import java.io.InputStream
import java.util.*

class JsonLoader(override val config: Properties): FileLoader {

    override fun loadFilesFromList(files: List<File>): List<FileDescriptor> {
        return emptyList()
    }

    override fun loadFilesInputStreams(files: List<File>): List<InputStream> {
        return emptyList()
    }
}