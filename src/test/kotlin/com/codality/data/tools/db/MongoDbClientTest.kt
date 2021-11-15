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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.*
import java.util.*

class MongoDbClientTest {

    @Test
    @Throws(Exception::class)
    fun setTest() {
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

    @Throws(Exception::class)
    @Test
    fun runnableTest() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("csv-metadata.yml")!!.file))
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
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MongoDbClientTest::class.java)
        private var hostname = "localhost"
        private var port = 27017
        private var mongoDbClient: MongoDbClient = MongoDbClient(hostname, port)
        private var database: MongoDatabase = mongoDbClient.getDatabase("testdb")

        @AfterAll
        fun teardown() {
            mongoDbClient.shutdown()
        }
    }
}