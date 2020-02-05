package org.sympatico.data.client.db;

import ai.grakn.redismock.RedisServer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sympatico.data.client.db.redis.RedisDbClient;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RedisDbClientTest {

    private static final Properties config = new Properties();

    private final ConcurrentLinkedQueue<Pair<String, byte[]>> redisQueue = new ConcurrentLinkedQueue<>();

    private static RedisServer server;
    private static RedisDbClient redis;

    @BeforeClass
    public static void setUp() throws Exception {
        server = RedisServer.newRedisServer(6688);
        server.start();
        redis = new RedisDbClient(
                ClientResources.builder()
                        .build(),
                RedisURI.builder()
                        .withHost(server.getHost())
                        .withPort(server.getBindPort())
                        .build());
    }

    @Test
    public void setTest() throws JSONException {
        JSONObject json1 = new JSONObject("{id: '1', arrayObj: {keyObj: 'value1'}}");
        JSONObject json2 = new JSONObject("{id: '2', arrayObj: {keyObj: 'value3'}}");
        JSONObject json3 = new JSONObject("{id: '3', arrayObj: {keyObj: 'value5'}}");
        redis.set("1", json1.get("arrayObj").toString().getBytes());
        redis.set("2", json1.get("arrayObj").toString().getBytes());
        redis.set("3", json1.get("arrayObj").toString().getBytes());
        Assert.assertEquals("{\"keyObj\":\"value1\"}", new String(redis.get("2")));
    }

    @AfterClass
    public static void teardown() throws Exception {
        server.stop();
    }

}
