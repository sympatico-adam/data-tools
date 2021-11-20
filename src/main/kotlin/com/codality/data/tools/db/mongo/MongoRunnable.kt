package com.codality.data.tools.db.mongo

import com.fasterxml.jackson.core.JsonParseException
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.slf4j.LoggerFactory
import com.codality.data.tools.Utils
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource
import kotlin.time.measureTime
import kotlin.time.milliseconds

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
                    parseQueueMessage(queue.poll())
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

    private fun parseQueueMessage(messagePair: Pair<String, ByteArray>) {
        val collection = messagePair.first
        val byteArray = messagePair.second
        if (byteArray.isEmpty()) {
            Thread.sleep(100)
        } else {
            try {
                val json = Utils.deserializeJsonByteArray(byteArray)
                if (json.isJsonObject && !json.isJsonNull) {
                    val jsonObject = Utils.deserializeJsonObject(json)
                    //LOG.info("json loaded:\n${jsonObject}\n")
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


    @ExperimentalTime
    fun shutdown() {
        LOG.info("shutting down mongo runnable...")
        isShuttingDown.set(true)
        val elapsed = measureTime {
            Thread.sleep(100)
        }

        while (queue.isNotEmpty() && elapsed < Duration.milliseconds(10000)) {
            if (queue.isNotEmpty()) {
                parseQueueMessage(queue.poll())
                LOG.debug("Messages left in queue: ${queue.size}")
            } else
                break
        }
    }

    companion object {
        private val isShuttingDown = AtomicBoolean(false)
    }
}