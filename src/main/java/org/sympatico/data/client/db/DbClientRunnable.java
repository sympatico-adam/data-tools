package org.sympatico.data.client.db;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class DbClientRunnable implements Runnable {

    private static final Logger LOG  = LoggerFactory.getLogger(DbClientRunnable.class);
    private static final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    private final DbClient dbClient;
    private final ConcurrentLinkedQueue<Pair<String, byte[]>> queue;

    DbClientRunnable(DbClient dbClient, ConcurrentLinkedQueue<Pair<String, byte[]>> queue) {
        this.dbClient = dbClient;
        this.queue = queue;
    }

    @Override
    public void run() {
        while (!isShuttingDown.get()) {
            try {
                if (queue.isEmpty()) {
                    Thread.sleep(1000L);
                } else {
                    final Pair<String, byte[]> line = queue.poll();
                    if (line != null) {
                        dbClient.set(line.getKey(), line.getValue());
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
        dbClient.shutdown();
        isShuttingDown.set(true);
    }
}
