package org.sympatico.client.db.producer;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.sympatico.client.db.FilmDataProcessor;
import org.sympatico.client.file.CsvFile;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CsvFileTest {

    private static final ConcurrentLinkedQueue<Pair<String, byte[]>> queue = new ConcurrentLinkedQueue<>();

    private final Properties config = new Properties();
    private FilmDataProcessor filmProcessor;

    @Before
    public void setup() throws Exception {
        config.load(new FileInputStream(new java.io.File("conf/client.test.properties")));
    }

    @Test
    public void uploadMeta() throws IOException {
        Map<String, Integer> map = new HashMap<>();
        map.put("id", 5);
        map.put("budget", 2);
        map.put("genre", 3);
        map.put("popularity", 10);
        map.put("company", 12);
        map.put("date", 14);
        map.put("revenue", 15);
        String metaRegex = config.getProperty("csv.regex");
        Boolean metaHeader = Boolean.parseBoolean(config.getProperty("csv.has.header"));
        CsvFile.jsonize(config.getProperty("csv.file"),
                map, metaHeader, "metadata", metaRegex, queue);
    }

    @Test
    public void jsonizeCsv() throws IOException {
        Map<String, Integer> map = new HashMap<>();
        map.put("id", 5);
        map.put("title", 20);
        map.put("budget", 2);
        map.put("genres", 3);
        map.put("popularity", 10);
        map.put("companies", 12);
        map.put("date", 14);
        map.put("revenue", 15);
        String regex = config.getProperty("csv.regex");
        Boolean hasHeader = Boolean.parseBoolean(config.getProperty("csv.has.header"));
        CsvFile.jsonize(config.getProperty("csv.file"),
                map, hasHeader, "test", regex, queue);
        while (!queue.isEmpty()) {
            System.out.println(new String(queue.poll().getValue()));
        }
    }
    @Test
    public void uploadRatings() throws IOException {
        Map<String, Integer> map = new HashMap<>();
        map.put("id", 1);
        map.put("rating", 2);
        String metaRegex = config.getProperty("movie.ratings.regex");
        Boolean metaHeader = Boolean.parseBoolean(config.getProperty("movie.ratings.has.header"));
        CsvFile.jsonize(config.getProperty("movie.ratings.file"),
                map, metaHeader, "ratings", metaRegex, queue);
    }
}