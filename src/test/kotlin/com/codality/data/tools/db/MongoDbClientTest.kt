package com.codality.data.tools.db

import com.codality.data.tools.CsvParser
import com.codality.data.tools.config.YamlProperties
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import org.bson.BsonDocument
import org.bson.Document
import org.codehaus.jettison.json.JSONArray
import org.codehaus.jettison.json.JSONException
import org.codehaus.jettison.json.JSONObject
import org.slf4j.LoggerFactory
import com.codality.data.tools.db.mongo.MongoDbClient
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.file.CsvLoader
import com.codality.data.tools.file.CsvLoaderTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.*
import java.nio.charset.StandardCharsets
import java.util.*

class MongoDbClientTest {

    @Test
    @Throws(JSONException::class)
    fun setTest() {
        val document1 = Document.parse("{test: '1', arrayObj: {keyObj: 'value1'}}")
        database.getCollection("TestCollection").insertOne(document1)
        val result = JSONArray()
        for (document in database.getCollection("TestCollection").find(Document())) {
            result.put(document.toJson())
        }
        println(result.toString())
        assertEquals("1", JSONObject(result[0].toString())["test"], "Incorrect return result")
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
        val result = JSONArray()
        val mongoCollection = database.getCollection("TestCollection2")
        println(mongoCollection.estimatedDocumentCount())
        for (document in mongoCollection.find().filter(
            Filters.eq(
                "arrayObj",
                BsonDocument.parse("{'keyObj': 'value1'}")
            )
        )) {
            result.put(JSONObject(document.toJson()))
            println(document.toJson())
        }
        println(result.toString())
        val jsonObject = result.getJSONObject(0)
        assertEquals(
            JSONObject("{\"keyObj\":\"value1\"}"),
            jsonObject["arrayObj"],
            "Incorrect return result"
        )
    }

    @Throws(Exception::class)
    @Test
    fun runnableTest() {
        val runner = MongoDocumentLoader(hostname, port)
        val queue = runner.getRunnableQueue()
        runner.startMongoDocumentLoader(4, "DocumentLoaderDB")
        val config = YamlProperties().load(File(CsvLoaderTest::class.java.classLoader.getResource("csv-metadata.yaml")!!.file))
        val regex = config.format!!.csv!!.regex
        val fields = config.format!!.csv!!.fieldsList
        val result = CsvParser(regex, fields)
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