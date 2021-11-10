package org.sympatico.data.client.db.redis

import io.lettuce.core.api.sync.RedisCommands
import org.apache.commons.lang3.tuple.Pair
import org.codehaus.jettison.json.JSONException
import org.codehaus.jettison.json.JSONObject
import org.slf4j.LoggerFactory
import org.sympatico.data.client.db.redis.RedisRunnable
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

class RedisRunnable(private val redisCommands: RedisCommands<String, ByteArray>) : Runnable {
    override fun run() {
        while (!isShuttingDown.get()) {
            try {
                if (queue.isEmpty()) {
                    Thread.sleep(1000L)
                } else {
                    val messagePair = queue.poll()
                    if (messagePair != null) {
                        try {
                            val json = JSONObject(String(messagePair.value, StandardCharsets.UTF_8))
                            redisCommands[messagePair.key] = messagePair.value
                        } catch (e: JSONException) {
                            e.printStackTrace()
                        }
                    } else {
                        LOG.warn("Writing queue is empty...")
                        Thread.sleep(1000)
                    }
                }
            } catch (e: InterruptedException) {
                LOG.error("Could not parse json object: $e")
                e.printStackTrace()
            }
        }
    }

    fun shutdown() {
        isShuttingDown.set(true)
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(RedisRunnable::class.java)
        private val isShuttingDown = AtomicBoolean(false)
        private val queue = ConcurrentLinkedQueue<Pair<String, ByteArray>>()
    }
}