package org.sympatico.data.client.db.mongo

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.codehaus.jettison.json.JSONObject
import java.util.concurrent.ConcurrentLinkedQueue
import java.lang.Runnable
import org.slf4j.LoggerFactory
import java.lang.InterruptedException
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.timer
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

class MongoRunnable(
    private val mongoDatabase: MongoDatabase,
    private val queue: ConcurrentLinkedQueue<Pair<String, ByteArray>>
) : Runnable {

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
            if (messagePair == null) {
                Thread.sleep(1000)
            } else {
                val objectMapper = ObjectMapper()
                val json = objectMapper.readTree(String(messagePair.second, StandardCharsets.UTF_8))
                if (json != null && !json.isEmpty(objectMapper.serializerProviderInstance)) {
                    mongoDatabase.getCollection(messagePair.first).insertOne(Document.parse(json.toString()))
                } else
                    LOG.info("dropping empty json object for key: ${messagePair.first}")

            }
        }
    }

    @OptIn(ExperimentalTime::class) fun shutdown() {
        LOG.info("shutting down mongo runnable...")
        isShuttingDown.set(true)
        val timeMark = TimeSource.Monotonic.markNow()
        while (!queue.isEmpty() || timeMark.elapsedNow() < Duration.Companion.milliseconds(5000))
            parseQueueMessage()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MongoRunnable::class.java)
        private val isShuttingDown = AtomicBoolean(false)
    }
}