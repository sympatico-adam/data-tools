package org.sympatico.data.client.db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sympatico.data.client.db.mongo.MongoDbClient;
import org.sympatico.data.client.file.CsvFile;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.mongodb.client.model.Filters.eq;

public class MongoDbClientTest {

    private static final Logger LOG  = LoggerFactory.getLogger(MongoDbClientTest.class);

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

        Map<String, Integer> map = new HashMap<>();
        map.put("id", 5);
        map.put("title", 20);
        map.put("budget", 2);
        map.put("genres", 3);
        map.put("popularity", 10);
        map.put("companies", 12);
        map.put("date", 14);
        map.put("revenue", 15);
        String inputFilePath = MongoDbClientTest.class.getClassLoader().getResource("movies_metadata_small.csv").getPath();
        File tempFile = File.createTempFile("test-csv-file", ",tmp");
        File outFile = File.createTempFile("test-csv-file", ",tmp");
        tempFile.deleteOnExit();
        outFile.deleteOnExit();
        long normalizedLineCount = CsvFile.writeNormalizedFile(inputFilePath, tempFile.getAbsolutePath());
        long parsedLineCount = CsvFile.jsonize(tempFile.getAbsolutePath(), map, config.getProperty("csv.regex"), outFile.getAbsolutePath());
        Assert.assertEquals(normalizedLineCount, parsedLineCount);
        long actualCount = 0L;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader((new FileInputStream(outFile))))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                actualCount++;
                queue.add(new ImmutablePair<>("test1", line.getBytes(StandardCharsets.UTF_8)));
            }
        }
        Assert.assertEquals(parsedLineCount, actualCount);

        JSONArray jsonObject = new JSONArray(new String(mongo.selectJson("test1")));
        long actual = jsonObject.length();

        for (int i = 0; i < jsonObject.length(); i++) {
            System.out.println(jsonObject.getString(i));
        }

        Assert.assertEquals(parsedLineCount, actual);
    }
}
