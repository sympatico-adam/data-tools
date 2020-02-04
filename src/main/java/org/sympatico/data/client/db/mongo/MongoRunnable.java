package org.sympatico.data.client.db.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.Document;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MongoRunnable implements Runnable {

    private static final Logger LOG  = LoggerFactory.getLogger(MongoRunnable.class);
    private static final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<Pair<String, byte[]>> queue;
    private final MongoDatabase mongoDatabase;

    public MongoRunnable(MongoDatabase mongoDatabase, ConcurrentLinkedQueue<Pair<String, byte[]>> queue) {
        this.queue = queue;
        this.mongoDatabase = mongoDatabase;
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
                            mongoDatabase.getCollection(messagePair.getKey()).insertOne(Document.parse(json.toString()));
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
