package org.sympatico.data.client.file

import org.junit.Before
import org.junit.Test
import org.slf4j.LoggerFactory
import org.sympatico.data.client.FileLoader
import org.sympatico.data.client.db.mongo.MongoDocumentLoader
import org.sympatico.data.client.json.JsonParser
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

    @Before
    @Throws(Exception::class)
    fun setup() {
        config.load(Objects.requireNonNull(JsonParserTest::class.java.classLoader.getResourceAsStream("client.test.properties")))
        loader.startMongoDocumentLoader(4, "testdb")
    }

  @ExperimentalTime
  @Test
  fun loadJsonFiles() {
      val files = FileLoader.loadFilesFromPath("src/test/resources/", "json")
      val parser = JsonParser()
      val queue = loader.getRunnableQueue()
      //      val testdb = mongo.getDatabase("testdb")
      files.forEach { file ->
          val jsonPair =  parser.parseJsonFile(file)
          queue.add(jsonPair.first to jsonPair.second)
    //          LOG.info("Document count: ${mongo.getDatabase("DocumentLoaderDB").getCollection("jsonCollection").countDocuments()}")
      }
      loader.shutdown()
  }

    companion object {
        private val LOG = LoggerFactory.getLogger(JsonParserTest::class.java)
    }
}