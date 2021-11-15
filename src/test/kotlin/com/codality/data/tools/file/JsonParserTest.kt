package com.codality.data.tools.file

import org.slf4j.LoggerFactory
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.JsonParser
import com.codality.data.tools.config.YamlProperties
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import java.io.File
import kotlin.time.ExperimentalTime
import org.junit.jupiter.api.Test

class JsonParserTest {
    private val server = MongoServer(MemoryBackend())
    val LOG = LoggerFactory.getLogger(JsonParserTest::class.java)

    @ExperimentalTime
    @Test
    fun loadJsonFiles() {
        val config = YamlProperties().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yaml")!!.file))
        server.bind(config.db.mongo.host, config.db.mongo.port)
        val loader = MongoDocumentLoader(config)
        loader.startMongoDocumentLoader()
        val files = FileLoader.findFilesInPath("data/", "json")
        val parser = JsonParser(config)
        val queue = loader.getRunnableQueue()
        //      val testdb = mongo.getDatabase("testdb")
        files.forEach { file ->
            val jsonPair = parser.parse(file)
            queue.add(jsonPair)
        }
        loader.shutdown()
        server.shutdown()
    }
}