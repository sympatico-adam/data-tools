package org.sympatico.client.db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.bson.Document;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistries;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import static com.mongodb.client.model.Filters.eq;

public class MongoTest {

    private static final Properties config = new Properties();
    private static Mongo mongo;
    private static MongoServer server;

    @BeforeClass
     public static void setup() throws Exception {
        server = new MongoServer(new MemoryBackend());

        // optionally: server.enableSsl(key, keyPassword, certificate);

        // bind on a random local port
        InetSocketAddress serverAddress = server.bind();
        String properties = "conf/client.test.properties";
        config.load(new FileInputStream(new File(properties)));

        final MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(
                        new ConnectionString("mongodb://" + serverAddress.getHostString() + ":" + serverAddress.getPort()))
                .codecRegistry(CodecRegistries.fromCodecs(
                        new BsonArrayCodec(),
                        new DocumentCodec(),
                        new BsonStringCodec(),
                        new StringCodec(),
                        new LongCodec(),
                        new IntegerCodec(),
                        new MapCodec(),
                        new BsonBooleanCodec(),
                        new BsonDocumentCodec(),
                        new PatternCodec()))
                .build();
        mongo = new Mongo(mongoClientSettings, config.getProperty("mongo.database.name"));
    }

    @AfterClass
    public static void teardown() {
        mongo.close();
        server.shutdown();
    }

    @Test
    public void a_set() throws IOException, JSONException {
        Document document1 = Document.parse("{test: '1', arrayObj: {keyObj: 'value1'}}");
        mongo.putJson("test", document1.toJson().getBytes());
        byte[] result = mongo.selectJsonAll("test");
        JSONObject json = new JSONObject(new JSONArray(new String(result)).get(0).toString());
        Assert.assertEquals("Incorrect return result", "1", json.get("test"));
    }

    @Test
    public void b_getFilter() throws JSONException, IOException {
        Document document1 = Document.parse("{test: '1', arrayObj: {keyObj: 'value1'}}");
        Document document2 = Document.parse("{test: '2', arrayObj: {keyObj: 'value3'}}");
        Document document3 = Document.parse("{test: '3', arrayObj: {keyObj: 'value5'}}");
        Document[] collection = new Document[3];
        collection[0] = document1;
        collection[1] = document2;
        collection[2] = document3;
        mongo.putCollection("test", collection);
        byte[] result = mongo.selectJsonWhere("test",  eq("test", "1"));
        JSONObject json = new JSONObject(new JSONArray(new String(result)).get(0).toString());
        Assert.assertEquals("Incorrect return result", "1", json.get("test"));
    }
}
