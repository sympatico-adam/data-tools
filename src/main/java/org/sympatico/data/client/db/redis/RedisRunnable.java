package org.sympatico.data.client.db.redis;

import com.mongodb.client.MongoDatabase;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

public class             RedisRunnable implements Runnable {

    private static final Logger LOG  = LoggerFactory.getLogger(RedisRunnable.class);
    private static final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    private static final ConcurrentLinkedQueue<Pair<String, byte[]>> queue = new ConcurrentLinkedQueue<>();
    private final RedisCommands<String, byte[]> redisCommands;

    public RedisRunnable(RedisCommands<String, byte[]> redisCommands) {
        this.redisCommands = redisCommands;
    }

    @Override
    public void run() {
        while (!isShuttingDown.get()) {
            try {
                if (queue.isEmpty()) {
                    Thread.sleep(1000L);
                } else {
                    final Pair<String, byte[]> messagePair = queue.poll();
                    if (messagePair != null) {
                        try {
                            JSONObject json = new JSONObject(new String(messagePair.getValue(), UTF_8));
                            redisCommands.set(messagePair.getKey(), messagePair.getValue());
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    } else {
                        LOG.warn("Writing queue is empty...");
                        Thread.sleep(1000);
                    }
                }
            }  catch (InterruptedException e) {
                LOG.error("Could not parse json object: " + e);
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        isShuttingDown.set(true);
    }
}
