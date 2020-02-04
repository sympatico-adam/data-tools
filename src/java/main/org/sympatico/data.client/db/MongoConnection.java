package org.sympatico.client.db;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

class MongoConnection {

    private static MongoClient mongoClient;

    protected synchronized static MongoClient connect(MongoClientSettings mongoClientSettings) {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(mongoClientSettings);
        }
        return mongoClient;
    }

    protected static void close() {
        mongoClient.close();
    }
}