package org.sympatico.client.db;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;

class RedisConnection {

    private StatefulRedisConnection<byte[], byte[]> connection;
    private RedisClient redisClient;

    protected RedisCommands<byte[], byte[]> connect(ClientResources clientResources, RedisURI redisURI) {
        redisClient = io.lettuce.core.RedisClient.create(clientResources, redisURI);
        connection = redisClient.connect(new ByteArrayCodec());
        return connection.sync();
    }

    protected synchronized void close(){
        if (connection.isOpen()) {
            connection.close();
            redisClient.shutdown();
        }
    }
}
