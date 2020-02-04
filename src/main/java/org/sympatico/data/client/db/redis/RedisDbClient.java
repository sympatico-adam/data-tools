package org.sympatico.data.client.db.redis;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import org.sympatico.data.client.db.DbClient;

import java.util.List;

import static org.sympatico.data.client.db.DbClientType.REDIS;

public class RedisDbClient extends DbClient {

    private RedisCommands<byte[], byte[]> commands;

    public RedisDbClient(ClientResources clientResources, RedisURI redisURI) {
        super(REDIS);
        commands = RedisConnection.connect(clientResources, redisURI);
    }

    public byte[] get(String key) {
        return commands.get(key.getBytes());
    }

    public void set(String key, byte[] value) {
        commands.set(key.getBytes(), value);
    }

    public void hset(String key, byte[] value) {
        byte[] id = String.valueOf(commands.incr(key.getBytes())).getBytes();
        commands.hset(id, id, value);
    }

    public Long hlen(String key) {
        return commands.hlen(key.getBytes());
    }

    public List<byte[]> hkeys(String key) {
        return commands.hkeys(key.getBytes());
    }

    public byte[] hget(String key, byte[] field) {
        return commands.hget(key.getBytes(), field);
    }

    public void hdel(String key, byte[]... fields) {
        commands.hdel(key.getBytes(), fields);
    }

    protected synchronized void shutdown(){
        RedisConnection.close();
    }
}
