package org.sympatico.client.db.runnable;

import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sympatico.client.db.Redis;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisRunnable implements Runnable {

    private static final Logger LOG  = LoggerFactory.getLogger(RedisRunnable.class);
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<Pair<String, byte[]>> queue;
    private ClientResources clientResources;
    private RedisURI redisURI;

    public RedisRunnable(ConcurrentLinkedQueue<Pair<String, byte[]>> queue, ClientResources clientResources, RedisURI redisURI) {
        this.queue = queue;
        this.clientResources = clientResources;
        this.redisURI = redisURI;
    }

    @Override
    public void run() {
        Redis redis = new Redis(clientResources, redisURI);
        while (!shutdown.get()) {
            try {
                if (queue.isEmpty()) {
                    Thread.sleep(1000L);
                } else {
                    final Pair<String, byte[]> line = queue.poll();
                    final String key = line.getKey();
                    try {
                        final JSONObject json = new JSONObject(new String(line.getValue(), StandardCharsets.UTF_8));
                        redis.hset(key,
                                json.toString().getBytes());
                    } catch (JSONException e) {
                        LOG.warn("Could not parse json object: " + e + "\n" + key + ": " + new String(line.getValue()));
                    }
                }
            }  catch (Exception e) {
                LOG.error("Could not parse json object: " + e);
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        shutdown.set(true);
    }
}
