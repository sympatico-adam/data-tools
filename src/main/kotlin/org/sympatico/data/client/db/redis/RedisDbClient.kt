package org.sympatico.data.client.db.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.RedisURI
import org.sympatico.data.client.db.redis.RedisDbClient

class RedisDbClient {
    @Synchronized
    fun connect(clientResources: ClientResources, redisURI: RedisURI): RedisClient? {
        if (redisClient == null) {
            redisClient = RedisClient.create(clientResources, redisURI)
        }
        return redisClient
    }

    @Synchronized
    fun shutdown() {
        redisClient!!.shutdown()
    }

    companion object {
        private var redisClient: RedisClient? = null
    }
}