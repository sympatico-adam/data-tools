package org.sympatico.data.client.db.mongo;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.util.concurrent.atomic.AtomicBoolean;

class MongoConnection {

    private static MongoClient mongoClient;
    private static final AtomicBoolean connected = new AtomicBoolean(false);

    private MongoConnection() {}

    protected synchronized static MongoClient connect(MongoClientSettings mongoClientSettings) {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(mongoClientSettings);
            connected.set(true);
        }
        return mongoClient;
    }

    protected synchronized static void close() {
        if (connected.getAndSet(false))
            mongoClient.close();
    }
}