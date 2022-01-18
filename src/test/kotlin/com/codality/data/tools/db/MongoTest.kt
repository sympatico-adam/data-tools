package com.codality.data.tools.db

import com.codality.data.tools.Utils
import com.codality.data.tools.config.ParserConf
import com.codality.data.tools.db.client.Mongo
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.file.CsvLoaderTest
import com.codality.data.tools.parser.FileFormat
import com.codality.data.tools.parser.FileParser
import com.google.gson.JsonPrimitive
import com.mongodb.client.model.Filters
import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.ExperimentalTime
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

class MongoTest {


    companion object {
        private val LOG = LoggerFactory.getLogger(MongoTest::class.java)
        private val config =
            ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("mongo-config.yml")!!.file))
        val mongo = Mongo(config.db.mongo.host, config.db.mongo.port)
    }

    @Test
    @Throws(Exception::class)
    fun setTest() {
        val database = mongo.getDatabase("testdb")
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
            "Incorrect return result"
        )
    }

    @Test
    fun filterTest() {
        val database = mongo.getDatabase("testdb")
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
    @Test
    fun runnableTest() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("csv-files.yml")!!.file))
        val csvParser = FileParser(FileFormat.CSV, config)
        val csvQueue = ConcurrentLinkedQueue<Pair<String, ByteArray>>()
        val runner = MongoDocumentLoader(config, csvQueue)
        runner.startMongoDocumentLoader()
        val files = csvParser.findFilesInPath("./data/", "csv")
        files.forEach { file ->
            csvQueue.add(file.nameWithoutExtension to csvParser.parse(file).toString().toByteArray())
        }
        val configJson =
            ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("json-files.yml")!!.file))
        val jsonParser = FileParser(FileFormat.JSON, config)
        val jsonFiles = jsonParser.findFilesInPath("./data/", "json")
        val jsonQueue = ConcurrentLinkedQueue<Pair<String, ByteArray>>()
        val jsonRunner = MongoDocumentLoader(configJson, jsonQueue)
        jsonRunner.startMongoDocumentLoader()
        jsonFiles.forEach { file ->
            jsonQueue.add(file.nameWithoutExtension to jsonParser.parse(file).toString().toByteArray())
        }
        jsonRunner.shutdown()
        LOG.info(
            "Collections: ${
                mongo.getDatabase(config.db.mongo.dbName)
                    .listCollectionNames().joinToString("\n")
            }"
        )
        val configReport =
            ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("report-files.yml")!!.file))
        val reportParser = FileParser(FileFormat.REPORT, config)
        val reportFiles = reportParser.findFilesInPath("./data/", "txt")
        val reportQueue = ConcurrentLinkedQueue<Pair<String, ByteArray>>()
        val reportRunner = MongoDocumentLoader(configReport, reportQueue)
        reportRunner.startMongoDocumentLoader()
        reportFiles.forEach { file ->
            reportQueue.add(file.nameWithoutExtension to reportParser.parse(file).toString().toByteArray())
        }
        reportRunner.shutdown()
        LOG.info(
            "Collections: ${
                mongo.getDatabase(config.db.mongo.dbName)
                    .listCollectionNames().joinToString("\n")
            }"
        )
    }

}
