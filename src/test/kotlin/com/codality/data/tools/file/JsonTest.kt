package com.codality.data.tools.file

import org.slf4j.LoggerFactory
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.config.ParserConf
import com.codality.data.tools.parser.FileFormat
import com.codality.data.tools.parser.FileParser
import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.ExperimentalTime
import org.junit.jupiter.api.Test

class JsonTest {


    companion object {
        private val LOG = LoggerFactory.getLogger(JsonTest::class.java)
    }

    @ExperimentalTime
    @Test
    fun loadJsonFiles() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
        val parser = FileParser(FileFormat.JSON, config)
        val queue = ConcurrentLinkedQueue<Pair<String, ByteArray>>()
        val loader = MongoDocumentLoader(config, queue)
        loader.startMongoDocumentLoader()
        val files = parser.findFilesInPath("src/test/resources/", "json")
        files.forEach { file ->
            LOG.info("parsing test file: ${file.nameWithoutExtension}")
            queue.add(file.nameWithoutExtension to parser.parse(file).toString().toByteArray())
        }
        loader.shutdown()
    }
}