package com.codality.data.tools.file

import org.codehaus.jettison.json.JSONException
import org.codehaus.jettison.json.JSONObject
import org.slf4j.LoggerFactory
import com.codality.data.tools.file.FileLoader.Companion.CSV_REGEX_DEFAULT
import com.codality.data.tools.file.FileLoader.Companion.CSV_REGEX_PROPERTY
import java.io.*
import java.util.*

class CsvLoader(override val config: Properties): FileLoader {

    override fun loadFilesFromList(files: List<File>): List<FileDescriptor> {
        return emptyList()
    }

    override fun loadFilesInputStreams(files: List<File>): List<InputStream> {
        return emptyList()
    }

}