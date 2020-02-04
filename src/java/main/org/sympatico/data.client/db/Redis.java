package org.sympatico.client.db;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;

import java.util.List;

public class Redis {

    private static final RedisConnection redisConnection = new RedisConnection();

    private RedisCommands<byte[], byte[]> commands;
    private ClientResources clientResources;
    private RedisURI redisURI;

    public Redis(ClientResources clientResources, RedisURI redisURI) {
        this.clientResources = clientResources;
        this.redisURI = redisURI;
    }

    public void connect() {
        commands = redisConnection.connect(clientResources, redisURI);
    }

    public void set(String key, byte[] value) {
        commands.set(key.getBytes(), value);
    }

    public byte[] get(String key) {
        return commands.get(key.getBytes());
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

    public synchronized void close(){
       redisConnection.close();
    }
}
