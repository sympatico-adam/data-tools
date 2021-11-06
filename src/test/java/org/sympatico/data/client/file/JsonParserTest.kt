package org.sympatico.data.client.file

import org.junit.Before
import org.junit.Test
import org.slf4j.LoggerFactory
import org.sympatico.data.client.FileLoader
import org.sympatico.data.client.db.mongo.MongoDocumentLoader
import org.sympatico.data.client.json.JsonParser
import java.util.*

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

  @Test
  fun loadJsonFiles() {
      val files = FileLoader.loadFilesFromPath("src/test/resources/zillow", "json")
      val parser = JsonParser()
      val queue = loader.queue
      //      val testdb = mongo.getDatabase("testdb")
      files.forEach { file ->
          val jsonPair =  parser.parseJsonFile(file)
          queue.add(jsonPair.first to
             jsonPair.second.toByteArray())
    //          LOG.info("Document count: ${mongo.getDatabase("DocumentLoaderDB").getCollection("jsonCollection").countDocuments()}")
      }
      loader.shutdown()
  }

    companion object {
        private val LOG = LoggerFactory.getLogger(JsonParserTest::class.java)
    }
}