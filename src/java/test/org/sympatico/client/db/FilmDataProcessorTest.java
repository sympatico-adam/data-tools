package org.sympatico.client.db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistries;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FilmDataProcessorTest {

    private Properties config = new Properties();
    private FilmDataProcessor filmProcessor;
    private Mongo mongo;
    private Redis redis;


    @Before
    public void setup() throws Exception {
        String properties = "conf/client.test.properties";
        config.load(new FileInputStream(new File(properties)));

        final MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(
                        new ConnectionString("mongodb://" + config.getProperty("mongo.host") + ":" + config.getProperty("mongo.port")))
                .codecRegistry(CodecRegistries.fromCodecs(
                        new BsonArrayCodec(),
                        new DocumentCodec(),
                        new BsonStringCodec(),
                        new StringCodec(),
                        new LongCodec(),
                        new IntegerCodec(),
                        new MapCodec()))
                .build();

        redis = new Redis(ClientResources.builder().build(), RedisURI.builder()
                .withHost(config.getProperty("redis.host"))
                .withPort(Integer.parseInt(config.getProperty("redis.port")))
                .build());
        mongo = new Mongo(mongoClientSettings, config.getProperty("mongo.database.name"));

        config.load(new FileInputStream(new File(properties)));
        filmProcessor = new FilmDataProcessor(properties);
    }

    @After
    public void teardown() throws Exception {
        redis.close();
        mongo.close();
    }

    @Test
    public void fullTest() throws InterruptedException, IOException {
        try {
            filmProcessor.runProcessors();
            for(int i = 0; i < 300; i++) {
                Thread.sleep(10000L);
            }
        } finally {
            filmProcessor.shutdown();
        }
    }

    @Test
    public void dumpJson() throws IOException {
        ConcurrentLinkedQueue<Pair<String, byte[]>> queue = new ConcurrentLinkedQueue<>();
        // Executors.newSingleThreadExecutor()
        //        .submit(new JsonFieldCombinator("metadata", "ratings", true, queue));
        FileWriter fw = new FileWriter(new File("./metadata.json"));
        while (!queue.isEmpty()) {
            fw.write(new String(queue.poll().getValue(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void test() throws Exception {
        System.out.println(new JSONObject("{\"date\":\"1994-06-09\",\"revenue\":\"350448145\",\"popularity\":\"10.859292\",\"genre\":\"[{'id': 28, 'name': 'Action'}, {'id': 12, 'name': 'Adventure'}, {'id': 80, 'name': 'Crime'}]\",\"company\":\"[{'name': 'Twentieth Century Fox Film Corporation', 'id': 306}]\",\"id\":\"1637\",\"budget\":\"30000000\"}").toString(2));

    }
}