package org.sympatico.data.client.db.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;

public class RedisDbClient {

    private static RedisClient redisClient;

    public synchronized RedisClient connect(ClientResources clientResources, RedisURI redisURI) {
        if (redisClient == null) {
            redisClient = RedisClient.create(clientResources, redisURI);
        }
        return redisClient;

    }

    public synchronized void shutdown() {
        redisClient.shutdown();
    }
}
