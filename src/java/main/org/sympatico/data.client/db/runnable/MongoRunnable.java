package org.sympatico.client.db.runnable;

import com.mongodb.MongoClientSettings;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sympatico.client.db.Mongo;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MongoRunnable implements Runnable {

    private static final Logger LOG  = LoggerFactory.getLogger(MongoRunnable.class);
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<Pair<String, byte[]>> queue;

    private String dbName;
    private MongoClientSettings mongoClientSettings;

    public MongoRunnable(ConcurrentLinkedQueue<Pair<String, byte[]>> queue, String dbname, MongoClientSettings mongoClientSettings) {
        this.queue = queue;
        this.dbName = dbname;
        this.mongoClientSettings = mongoClientSettings;
    }

    @Override
    public void run() {
        Mongo mongo = new Mongo(mongoClientSettings, dbName);
        while (!shutdown.get()) {
            try {
                if (queue.isEmpty()) {
                    Thread.sleep(1000L);
                } else {
                    final Pair<String, byte[]> line = queue.poll();
                    if (line != null) {
                        mongo.putJson(line.getKey(), line.getValue());
                    } else {
                        LOG.warn("Mongo queue returned null item...");
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
