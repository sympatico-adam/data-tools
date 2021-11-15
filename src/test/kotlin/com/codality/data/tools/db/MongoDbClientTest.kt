package com.codality.data.tools.db

import com.codality.data.tools.CsvParser
import com.codality.data.tools.Utils
import com.codality.data.tools.config.ParserConf
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import org.bson.BsonDocument
import org.bson.Document
import org.slf4j.LoggerFactory
import com.codality.data.tools.db.mongo.MongoDbClient
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.file.CsvLoaderTest
import com.google.gson.JsonPrimitive
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.*
import java.util.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

class MongoDbClientTest {

    private val LOG = LoggerFactory.getLogger(MongoDbClientTest::class.java)

    @Test
    @Throws(Exception::class)
    fun setTest() {
        val server = MongoServer(MemoryBackend())
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
        server.bind(config.db.mongo.host, config.db.mongo.port)
        val mongoDbClient = MongoDbClient(config.db.mongo.host, config.db.mongo.port)
        val database = mongoDbClient.getDatabase("testdb")
        val document1 = Document.parse("{test: '1', arrayObj: {keyObj: 'value1'}}")
        database.getCollection("TestCollection").insertOne(document1)
        val result = mutableListOf<ByteArray>()
        for (document in database.getCollection("TestCollection").find(Document())) {
            result.add(document.toJson().encodeToByteArray())
        }
        LOG.info(result.toString())
        assertEquals(
            JsonPrimitive("1"),
            Utils.deserializeJsonByteArray(result[0]).asJsonObject.get("test").asJsonPrimitive,
            "Incorrect return result")
        server.shutdown()
    }

    @Test
    fun filterTest() {
        val server = MongoServer(MemoryBackend())
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
        server.bind(config.db.mongo.host, config.db.mongo.port)
        val mongoDbClient = MongoDbClient(config.db.mongo.host, config.db.mongo.port)
        val database = mongoDbClient.getDatabase("testdb")
        val document1 = Document.parse("{arrayObj: {keyObj: 'value1'}}}")
        val document2 = Document.parse("{arrayObj: {keyObj: 'value3'}}}")
        val document3 = Document.parse("{arrayObj: {keyObj: 'value5'}}}")
        val collection: MutableList<Document> = ArrayList()
        collection.add(document1)
        collection.add(document2)
        collection.add(document3)
        database.getCollection("TestCollection2").insertMany(collection)
        val result = mutableListOf<ByteArray>()
        val mongoCollection = database.getCollection("TestCollection2")
        LOG.info(mongoCollection.estimatedDocumentCount().toString())
        for (document in mongoCollection.find().filter(
            Filters.eq(
                "arrayObj",
                BsonDocument.parse("{'keyObj': 'value1'}")
            )
        )) {
            result.add(document.toJson().encodeToByteArray())
        }
        val jsonResult = Utils.deserializeJsonByteArray(result[0])
        LOG.info(jsonResult.toString())
        assertEquals(
            "{\"keyObj\":\"value1\"}",
            jsonResult.asJsonObject.get("arrayObj").toString(),
            "Incorrect return result"
        )
        server.shutdown()
    }

    @Throws(Exception::class)
    @Test
    fun runnableTest() {
        val server = MongoServer(MemoryBackend())
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
`        server.bind(config.db.mongo.host, config.db.mongo.port)
        val mongoDbClient = MongoDbClient(config.db.mongo.host, config.db.mongo.port)
        val database = mongoDbClient.getDatabase("testdb")
        val runner = MongoDocumentLoader(config)
        val queue = runner.getRunnableQueue()
        runner.startMongoDocumentLoader()
        val result = CsvParser(config)
            .parse(
                File(
                    CsvLoaderTest::class.java.classLoader.getResource("movies_metadata_small_fixed.csv")!!.file
                )
            )
        queue.add(result)
        val actual = mongoDbClient.getDatabase("movies_metadata_small")
        LOG.info(actual.toString())
        //assertTrue(parsedLineCount < actual + 5000)
        server.shutdown()
    }

}