package org.sympatico.data.client.db;

import ai.grakn.redismock.RedisServer;
import com.google.common.base.Utf8;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
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

import static java.nio.charset.StandardCharsets.UTF_8;

public class RedisDbClientTest {

    private static final Properties config = new Properties();

    private final ConcurrentLinkedQueue<Pair<String, byte[]>> redisQueue = new ConcurrentLinkedQueue<>();

    private static RedisServer server;
    private static RedisCommands<byte[], byte[]> connection;
    private static RedisClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        server = RedisServer.newRedisServer(6688);
        server.start();
        RedisDbClient redisDbClient = new RedisDbClient();
        connection = redisDbClient.connect(ClientResources.builder()
                        .build(),
                RedisURI.builder()
                        .withHost(server.getHost())
                        .withPort(server.getBindPort())
                        .build())
                .connect(new ByteArrayCodec())
                .sync();

    }


    @Test
    public void setTest() throws JSONException {
        JSONObject json1 = new JSONObject("{id: '1', arrayObj: {keyObj: 'value1'}}");
        JSONObject json2 = new JSONObject("{id: '2', arrayObj: {keyObj: 'value3'}}");
        JSONObject json3 = new JSONObject("{id: '3', arrayObj: {keyObj: 'value5'}}");
        connection.set("1".getBytes(UTF_8), json1.get("arrayObj").toString().getBytes(UTF_8));
        connection.set("2".getBytes(UTF_8), json1.get("arrayObj").toString().getBytes(UTF_8));
        connection.set("3".getBytes(UTF_8), json1.get("arrayObj").toString().getBytes(UTF_8));
        Assert.assertEquals("{\"keyObj\":\"value1\"}", new String(connection.get("2".getBytes(UTF_8))));
    }

    @AfterClass
    public static void teardown() throws Exception {
        server.stop();
    }

}
