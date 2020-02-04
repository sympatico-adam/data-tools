package org.sympatico.data.client.db.mongo;


import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.concurrent.atomic.AtomicBoolean;

public class MongoDbClient {

    private static final AtomicBoolean connected = new AtomicBoolean(false);
    private MongoClient mongoClient;

    public synchronized MongoClient connect(MongoClientSettings mongoClientSettings) {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(mongoClientSettings);
            connected.set(true);
        }
        return mongoClient;
    }

    public synchronized void shutdown() {
        if (connected.getAndSet(false))
            mongoClient.close();
    }
}
