package com.codality.data.tools.db

import com.codality.data.tools.Utils
import com.codality.data.tools.config.ParserConf
import com.mongodb.client.model.Filters
import org.bson.BsonDocument
import org.bson.Document
import org.slf4j.LoggerFactory
import com.codality.data.tools.db.mongo.MongoDbClient
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.file.CsvLoaderTest
import com.codality.data.tools.parser.FileParser.Companion.findFilesInPath
import com.codality.data.tools.parser.JsonParser
import com.google.gson.JsonPrimitive
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.*
import java.util.*
import kotlin.time.ExperimentalTime
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

class MongoDbClientTest {


    companion object {

        private val LOG = LoggerFactory.getLogger(MongoDbClientTest::class.java)
        private val config =
            ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("mongo-config.yml")!!.file))

    }

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

    @Test
    @Throws(Exception::class)
    fun setTest() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
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
    }

    @Test
    fun filterTest() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
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
    }

    @ExperimentalTime
    @Throws(Exception::class)
    fun runnableTest() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
        val parser = JsonParser(config)
        val runner = MongoDocumentLoader(config, parser.getQueue())
        runner.startMongoDocumentLoader()
        val files = findFilesInPath("data/", "json")
        files.forEach { file ->
            parser.parse(file, "test")
        }
        runner.shutdown()
       /* val configJson = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
        val jsonParser = JsonParser(configJson)
        val jsonFiles = findFilesInPath("/home/user/QubesIncoming/work/data/mozilla", "json")
        val jsonRunner = MongoDocumentLoader(configJson, jsonParser.getQueue())
        jsonRunner.startMongoDocumentLoader()
        jsonFiles.forEach { file ->
            jsonParser.parse(file)
        }
        jsonRunner.shutdown()
        LOG.info("Collections: ${
            MongoDbClient(config.db.mongo.host, config.db.mongo.port)
                .getDatabase(config.db.mongo.dbName)
                .listCollectionNames().joinToString("\n")
        }")*/
    }

}