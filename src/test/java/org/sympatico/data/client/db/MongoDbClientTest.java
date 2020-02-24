package org.sympatico.data.client.db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BSONObject;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.internal.CodecRegistryHelper;
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
import org.sympatico.data.client.file.CsvFileClient;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MongoDbClientTest {

    private static final Logger LOG  = LoggerFactory.getLogger(MongoDbClientTest.class);

    private static final Properties config = new Properties();
    private static MongoClient mongo;
    private static MongoDatabase database;
    private static MongoServer server;
    private static MongoClientSettings mongoClientSettings;
    private static CodecRegistry codecRegistry;

    @BeforeClass
     public static void setup() throws Exception {
        server = new MongoServer(new MemoryBackend());

        // optionally: server.enableSsl(key, keyPassword, certificate);

        // bind on a random local port
        InetSocketAddress serverAddress = server.bind();
        String properties = "client.test.properties";
        config.load(MongoDbClientTest.class.getClassLoader().getResourceAsStream("client.test.properties"));
        codecRegistry = CodecRegistries.fromCodecs(
                new BsonArrayCodec(),
                new DocumentCodec(),
                new BsonStringCodec(),
                new StringCodec(),
                new LongCodec(),
                new IntegerCodec(),
                new MapCodec(),
                new BsonBooleanCodec(),
                new BsonDocumentCodec(),
                new PatternCodec());
        mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(
                        new ConnectionString("mongodb://" + serverAddress.getHostString() + ":" + serverAddress.getPort()))
                .codecRegistry(codecRegistry)
                .build();
        mongo = new MongoDbClient().connect(mongoClientSettings);
        database = mongo.getDatabase("TestDB");
    }

    @AfterClass
    public static void teardown() {
        server.shutdown();
    }

    @Test
    public void setTest() throws JSONException {
        Document document1 = Document.parse("{test: '1', arrayObj: {keyObj: 'value1'}}");
        database.getCollection("TestCollection").insertOne(document1);
        JSONArray result = new JSONArray();
        for (Document document: database.getCollection("TestCollection").find(new Document())) {
            result.put(document.toJson());
        }
        System.out.println(result.toString());
        Assert.assertEquals("Incorrect return result", "1", new JSONObject(result.get(0).toString()).get("test"));
    }

    @Test
    public void getFilterTest() throws JSONException, IOException {
        Document document1 = Document.parse("{arrayObj: {keyObj: 'value1'}}}");
        Document document2 = Document.parse("{arrayObj: {keyObj: 'value3'}}}");
        Document document3 = Document.parse("{arrayObj: {keyObj: 'value5'}}}");
        List<Document> collection = new ArrayList<>();
        collection.add(document1);
        collection.add(document2);
        collection.add(document3);
        database.getCollection("TestCollection2").insertMany(collection);
        JSONArray result = new JSONArray();
        MongoCollection<Document> mongoCollection = database.getCollection("TestCollection2");
        System.out.println(mongoCollection.estimatedDocumentCount());
        for (Document document: mongoCollection.find().filter(eq("arrayObj",
                BsonDocument.parse("{'keyObj': 'value1'}")))) {
            result.put(new JSONObject(document.toJson()));
            System.out.println(document.toJson());
        }
        System.out.println(result.toString());
        JSONObject jsonObject = result.getJSONObject(0);
        Assert.assertEquals("Incorrect return result",
                new JSONObject("{\"keyObj\":\"value1\"}"),
                jsonObject.get("arrayObj"));
    }

    @Test
    public void runnableTest() throws Exception {

        MongoDocumentLoader runner = new MongoDocumentLoader();
        ConcurrentLinkedQueue<Pair<String, byte[]>> queue = runner.getQueue();
        runner.startMongoDocumentLoader(mongoClientSettings,"DocumentLoaderDB", 4);
        Map<Integer, String> map = new HashMap<>();
        map.put(5, "id");
        map.put(20, "title");
        map.put(2, "budget");
        map.put(3, "genres");
        map.put(10, "popularity");
        map.put(12, "companies");
        map.put(14, "date");
        map.put(15, "revenue");
        File tempFile = File.createTempFile("test-csv-file", ".tmp");
        tempFile.deleteOnExit();
        CsvFileClient csvFileClient = new CsvFileClient(map, config.getProperty("csv.regex"));
        long parsedLineCount = 0L;
        JSONArray jsonArray = new JSONArray();
        try (OutputStream tempOutputStream = new FileOutputStream(tempFile);
             InputStream inputStream = Objects.requireNonNull(MongoDbClientTest.class.getClassLoader().getResourceAsStream("movies_metadata_small.csv"))) {
            parsedLineCount = csvFileClient.jsonizeFileStream(inputStream, tempOutputStream);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(tempFile)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                queue.add(new ImmutablePair<>("TestCollection3", jsonObject.toString().getBytes(UTF_8)));
            }
        }
        Thread.sleep(5000L);
        long actual = mongo.getDatabase("DocumentLoaderDB").getCollection("TestCollection3").countDocuments();
        System.out.println("Test Collection: " + jsonArray.length() + "\n\n\n" + actual);

        for (int i = 0; i < jsonArray.length(); i++) {
            System.out.println(jsonArray.getString(i));
        }

        Assert.assertTrue(parsedLineCount < actual+5000);
    }
}
