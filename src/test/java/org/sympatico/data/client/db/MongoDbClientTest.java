package org.sympatico.data.client.db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.apache.commons.lang3.tuple.Pair;
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
import org.sympatico.data.client.db.mongo.MongoDbClient;
import org.sympatico.data.client.file.CsvFile;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;

public class MongoDbClientTest {

    private static final Properties config = new Properties();
    private static MongoDbClient mongo;
    private static MongoServer server;

    @BeforeClass
     public static void setup() throws Exception {
        server = new MongoServer(new MemoryBackend());

        // optionally: server.enableSsl(key, keyPassword, certificate);

        // bind on a random local port
        InetSocketAddress serverAddress = server.bind();
        String properties = "client.test.properties";
        config.load(MongoDbClientTest.class.getClassLoader().getResourceAsStream("client.test.properties"));

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
        mongo = new MongoDbClient(mongoClientSettings, config.getProperty("mongo.database.name"));
    }

    @AfterClass
    public static void teardown() {
        mongo.shutdown();
        server.shutdown();
    }

    @Test
    public void setTest() throws JSONException {
        Document document1 = Document.parse("{test: '1', arrayObj: {keyObj: 'value1'}}");
        mongo.putJson("test", document1.toJson().getBytes());
        byte[] result = mongo.selectJson("test");
        JSONObject json = new JSONObject(new JSONArray(new String(result)).get(0).toString());
        Assert.assertEquals("Incorrect return result", "1", json.get("test"));
    }

    @Test
    public void getFilterTest() throws JSONException, IOException {
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

    @Test
    public void runnableTest() throws Exception {

        ConcurrentLinkedQueue<Pair<String, byte[]>> queue = new ConcurrentLinkedQueue<>();
        DbClientRunner runner = new DbClientRunner(mongo, queue);
        runner.run(4);

        Map<String, Integer> fields = new HashMap<>();
        fields.put("id", 1);
        fields.put("rating", 2);
        String filename = MongoDbClientTest.class.getClassLoader().getResource("ratings_small.csv").getPath();

        File file = new File(filename);
        int lineCount = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            br.readLine();
            while (br.readLine() != null)
                lineCount++;
        }

        CsvFile.jsonize(filename, fields,true, "test1", ",", queue);

        while (!queue.isEmpty()) {
            Thread.sleep(1000);
        }

        Thread.sleep(5000);

        JSONArray jsonObject = new JSONArray(new String(mongo.selectJson("test1")));
        int actual = jsonObject.length();

        Assert.assertEquals(lineCount, actual);
    }
}
