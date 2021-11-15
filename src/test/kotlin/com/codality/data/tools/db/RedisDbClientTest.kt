package com.codality.data.tools.db

import ai.grakn.redismock.RedisServer
import com.codality.data.tools.Utils
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.resource.ClientResources
import com.codality.data.tools.db.redis.RedisDbClient
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class RedisDbClientTest {
    private val redisQueue = ConcurrentLinkedQueue<Pair<String, ByteArray>>()

    @Test
    @Throws(Exception::class)
    fun setTest() {
        val server: RedisServer = RedisServer.newRedisServer(6688)
        server.start()
        val redisDbClient = RedisDbClient()
        val connection: RedisCommands<ByteArray, ByteArray> = redisDbClient.connect(
            ClientResources.builder()
                .build(),
            RedisURI.builder()
                .withHost(server.host)
                .withPort(server.bindPort)
                .build()
        )!!.connect(ByteArrayCodec()).sync()
        val client: RedisClient? = null
        val json1 = Utils.deserializeJsonString("{id: '1', arrayObj: {keyObj: 'value1'}}")
        val json2 = Utils.deserializeJsonString("{id: '2', arrayObj: {keyObj: 'value3'}}")
        val json3 = Utils.deserializeJsonString("{id: '3', arrayObj: {keyObj: 'value5'}}")
        connection["1".toByteArray(StandardCharsets.UTF_8)] =
            json1.asJsonObject.get("arrayObj").toString()
                .toByteArray(StandardCharsets.UTF_8)
        connection["2".toByteArray(StandardCharsets.UTF_8)] =
            json1.asJsonObject.get("arrayObj").toString()
                .toByteArray(StandardCharsets.UTF_8)
        connection["3".toByteArray(StandardCharsets.UTF_8)] =
            json1.asJsonObject.get("arrayObj").toString()
                .toByteArray(StandardCharsets.UTF_8)
        assertEquals(String(connection["2".toByteArray(StandardCharsets.UTF_8)]), "{\"keyObj\":\"value1\"}")
        server.stop()
    }
}