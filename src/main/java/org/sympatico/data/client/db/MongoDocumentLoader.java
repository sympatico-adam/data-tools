package org.sympatico.data.client.db;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import org.apache.commons.lang3.tuple.Pair;
import org.sympatico.data.client.db.mongo.MongoDbClient;
import org.sympatico.data.client.db.mongo.MongoRunnable;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MongoDocumentLoader {

    private ExecutorService executorService;
    private MongoRunnable[] runnables;
    private static final ConcurrentLinkedQueue<Pair<String, byte[]>> queue = new ConcurrentLinkedQueue<>();

    public MongoDocumentLoader() {
        final MongoDocumentLoader daemonRef = this;
        Runtime.getRuntime().addShutdownHook(new Thread(daemonRef::shutdown));
    }

    public void startMongoDocumentLoader(MongoClientSettings mongoClientSettings, String database, int workerCount) {
        final MongoDbClient mongoDbClient = new MongoDbClient();
        MongoClient mongoClient = mongoDbClient.connect(mongoClientSettings);
        executorService = Executors.newFixedThreadPool(workerCount);
        runnables = new MongoRunnable[workerCount];
        for (int i = 0; i < workerCount; i++) {
            runnables[i] = new MongoRunnable(mongoClient.getDatabase(database), queue);
            executorService.submit(runnables[i]);
        }
    }

    public ConcurrentLinkedQueue<Pair<String, byte[]>> getQueue() {
        return queue;
    }
    private synchronized void shutdown() {
        if (!executorService.isShutdown())
            for (MongoRunnable runnable: runnables) {
                runnable.shutdown();
            }
            executorService.shutdown();
    }
}
