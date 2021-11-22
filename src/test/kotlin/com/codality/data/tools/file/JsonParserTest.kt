package com.codality.data.tools.file

import com.codality.data.tools.parser.FileParser
import org.slf4j.LoggerFactory
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.parser.JsonParser
import com.codality.data.tools.config.ParserConf
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import java.io.File
import kotlin.time.ExperimentalTime
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class JsonParserTest {


    companion object {

        private val LOG = LoggerFactory.getLogger(JsonParserTest::class.java)

    }

    private val config =
        ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("mongo-config.yml")!!.file))
    lateinit var server: MongoServer

    @BeforeEach
    fun setup() {
        server = MongoServer(MemoryBackend())
        server.bind(config.db.mongo.host, config.db.mongo.port)
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }
    @ExperimentalTime
    @Test
    fun loadJsonFiles() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
        val parser = JsonParser(config)
        val loader = MongoDocumentLoader(config, parser.getQueue())
        loader.startMongoDocumentLoader()
        val files = FileParser.findFilesInPath("src/test/resources/", "json")
        files.forEach { file ->
            LOG.info("parsing test file: ${file.nameWithoutExtension}")
            parser.parse(file, file.nameWithoutExtension)
        }
        loader.shutdown()
    }
}