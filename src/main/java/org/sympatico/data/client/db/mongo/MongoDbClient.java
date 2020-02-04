package org.sympatico.data.client.db.mongo;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sympatico.data.client.db.DbClient;

import static java.util.Arrays.asList;
import static org.sympatico.data.client.db.DbClientType.MONGO;

public class MongoDbClient extends DbClient {

    private static final Logger LOG  = LoggerFactory.getLogger(MongoDbClient.class);

    private MongoDatabase mongoDatabase;

    public MongoDbClient(MongoClientSettings mongoClientSettings, String databaseName) {
        super(MONGO);
        MongoClient client = MongoConnection.connect(mongoClientSettings);
        mongoDatabase = client.getDatabase(databaseName);
    }

    public void set(String key, byte[] json) {
        putJson(key, json);
    }

    public byte[] get(String key)  {
        return selectJson(key);
    }
    /**
     * Insert a list of documents into a collection
     * @param collectionName
     * @param documents
     */
    public void putCollection(String collectionName, Document[] documents) {
        mongoDatabase.getCollection(collectionName, Document.class).insertMany(asList(documents));
    }

    /**
     * Insert a single document into a collection
     * @param collectionName
     * @param document
     */
    public void putDocument(String collectionName, Document document) {
        mongoDatabase.getCollection(collectionName).insertOne(document);
    }

    /**
     * Returns a MongoCollection of the collection
     * @param collectionName
     * @return
     */
    public MongoCollection<Document> selectCollection(String collectionName) {
        return mongoDatabase.getCollection(collectionName);
    }

    /**
     * Insert json formatted text as a document into a collection
     * @param collectionName
     * @param jsonBytes
     */
    public void putJson(String collectionName, byte[] jsonBytes) {
        selectCollection(collectionName).insertOne(Document.parse(new String(jsonBytes)));
    }

    /**
     * Get all documents from the collection
     * @param collectionName
     * @return
     */
    public byte[] selectJson(String collectionName) {
        JSONArray result = new JSONArray();
        for (Document document: selectCollection(collectionName).find(new Document())) {
            result.put(document.toJson());
        }
        return result.toString().getBytes();
    }

    /**
     * Provide a Bson formatted query, such as _eq("field", "value")_ to perform basic filtering
     * @param collectionName
     * @param query
     * @return
     */
    public byte[] selectJsonWhere(String collectionName, Bson query) {
        JSONArray result = new JSONArray();
        for (Document document: mongoDatabase.getCollection(collectionName).find(new Document()).filter(query)) {
            result.put(document.toJson());
        }
        return result.toString().getBytes();
    }

    public void createCollection(String collectionName) {
        mongoDatabase.createCollection(collectionName);
    }

    public synchronized void shutdown() {
        MongoConnection.close();
    }
}
