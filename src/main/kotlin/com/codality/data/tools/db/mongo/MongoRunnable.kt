package com.codality.data.tools.db.mongo

import com.fasterxml.jackson.core.JsonParseException
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.slf4j.LoggerFactory
import com.codality.data.tools.Utils
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

class MongoRunnable(
    private val mongoDatabase: MongoDatabase,
    private val queue: ConcurrentLinkedQueue<Pair<String, ByteArray>>
) : Runnable {

    private val LOG = LoggerFactory.getLogger(MongoRunnable::class.java)

    override fun run() {
        while (!isShuttingDown.get()) {
            try {
                if (queue.isEmpty()) {
                    Thread.sleep(1000L)
                } else {
                    parseQueueMessage()
                }
            } catch (e: InterruptedException) {
                LOG.error("thread execution interrupted: $e")
                e.printStackTrace()
            } catch (e: JsonParseException) {
                LOG.error("json parsing error: $e")
                e.printStackTrace()
            }
        }
    }

    private fun parseQueueMessage() {
        while (queue.isNotEmpty()) {
            val messagePair = queue.poll()
            val collection = messagePair.first
            val byteArray = messagePair.second
            if (byteArray.isEmpty()) {
                Thread.sleep(1000)
            } else {
                try {
                    val json = Utils.deserializeJsonByteArray(byteArray)
                    if (json.isJsonObject && !json.isJsonNull) {
                        val jsonObject = Utils.deserializeJsonObject(json)
                        LOG.info("json loaded:\n${jsonObject}\n")
                        mongoDatabase.getCollection(collection)
                            .insertOne(Document(Utils.jsonToMap(jsonObject)))
                    } else if (json.isJsonArray && !json.isJsonNull && !json.asJsonArray.isEmpty) {
                        val documents = mutableListOf<Document>()
                        Utils.deserializeJsonArray(json)
                            .fold(documents) { acc, element ->
                                if (element.isJsonObject)
                                    acc.add(Document(Utils.jsonToMap(element)))
                                acc
                            }
                        if (documents.isNotEmpty())
                            mongoDatabase.getCollection(collection).insertMany(documents)

                    } else
                        LOG.debug("dropping empty json object for collection [ $collection ]\n")
                } catch (e: Exception) {
                    LOG.error("bad json for collection [ $collection ]: \n${String(byteArray)}\n")
                    e.printStackTrace()
                }
            }
        }
    }


    @ExperimentalTime
    fun shutdown() {
        LOG.info("shutting down mongo runnable...")
        isShuttingDown.set(true)
        val timeMark = TimeSource.Monotonic.markNow()
        while (!queue.isEmpty() || timeMark.elapsedNow() < Duration.Companion.milliseconds(5000))
            parseQueueMessage()
    }

    companion object {
        private val isShuttingDown = AtomicBoolean(false)
    }
}