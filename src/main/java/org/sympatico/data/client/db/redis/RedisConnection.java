package org.sympatico.data.client.db.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;

class RedisConnection {

    private static StatefulRedisConnection<byte[], byte[]> connection;
    private static RedisClient redisClient;

    private RedisConnection() {}

    protected synchronized static RedisCommands<byte[], byte[]> connect(ClientResources clientResources, RedisURI redisURI) {
        if (redisClient == null) {
            redisClient = RedisClient.create(clientResources, redisURI);
            connection = redisClient.connect(new ByteArrayCodec());
        }
        return connection.sync();
    }

    public synchronized static void close() {
        if (connection.isOpen()) {
            connection.close();
            redisClient.shutdown();
        }
    }

}
