package com.codality.data.tools.file

import org.slf4j.LoggerFactory
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.JsonParser
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.time.ExperimentalTime

class JsonParserTest {
    private val config = Properties()

    /*private val server = MongoServer(MemoryBackend())
    private var serverAddress = server.bind()*/
    private val hostname = "localhost"
    private val port = 27017
    private val loader = MongoDocumentLoader(hostname, port)
    /*private val mongo = MongoDbClient(hostname, port).getClient()*/

    @ExperimentalTime
    @Test
    fun loadJsonFiles() {
        loader.startMongoDocumentLoader(4, "testdb")
        val files = FileLoader.findFilesInPath("src/test/resources/", "json")
        val parser = JsonParser()
        val queue = loader.getRunnableQueue()
        //      val testdb = mongo.getDatabase("testdb")
        files.forEach { file ->
            val jsonPair = parser.parseJsonFile(file)
            queue.add(jsonPair.first to jsonPair.second)
            //          LOG.info("Document count: ${mongo.getDatabase("DocumentLoaderDB").getCollection("jsonCollection").countDocuments()}")
        }
        loader.shutdown()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(JsonParserTest::class.java)
    }
}