package org.sympatico.client.db;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sympatico.client.db.runnable.MongoRunnable;
import org.sympatico.client.db.runnable.RedisRunnable;
import org.sympatico.client.file.CsvFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class FilmDataProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FilmDataProcessor.class);
    private static final Properties config = new Properties();

    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    private static RedisRunnable[] redisRunnables;
    private static MongoRunnable[] mongoRunnables;

    private static ExecutorService monitor = Executors.newSingleThreadExecutor();
    private static ExecutorService jsonFieldExecutor = Executors.newSingleThreadExecutor();

    private final ConcurrentLinkedQueue<Pair<String, byte[]>> redisQueue =
            new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Pair<String, byte[]>> mongoQueue =
            new ConcurrentLinkedQueue<>();

    private static final String RATINGS = "ratings";
    private static String METADATA = "metadata";

    public FilmDataProcessor(String properties) throws IOException {
        redisRunnables = new RedisRunnable[Integer.parseInt(config.getProperty("redis.worker.pool.size"))];
        mongoRunnables = new MongoRunnable[Integer.parseInt(config.getProperty("mongo.worker.pool.size"))];
        monitor.submit(new Monitor());
    }

    protected void runProcessors() throws IOException {
        runMetaFileWorkers(redisQueue);
        runRatingFileWorkers(redisQueue);
    }

    protected void runRatingFileWorkers(ConcurrentLinkedQueue<Pair<String, byte[]>> redisQueue) throws IOException {
        Map<String, Integer> map = new HashMap<>();
        map.put("id", 1);
        map.put("rating", 2);
        String ratingsRegex = config.getProperty("movie.ratings.regex");
        Boolean ratingsHeader = Boolean.parseBoolean(config.getProperty("movie.ratings.has.header"));
        CsvFile.jsonize(config.getProperty("movie.ratings.file"),
                map, ratingsHeader, RATINGS, ratingsRegex, redisQueue);
        LOG.info("Ratings data has been added to the redis queue");
    }

    protected void runMetaFileWorkers(ConcurrentLinkedQueue<Pair<String, byte[]>> redisQueue) throws IOException {
        Map<String, Integer> map = new HashMap<>();
        map.put("id", 5);
        map.put("title", 20);
        map.put("budget", 2);
        map.put("genres", 3);
        map.put("popularity", 10);
        map.put("companies", 12);
        map.put("date", 14);
        map.put("revenue", 15);
        String metaRegex = config.getProperty("csv.regex");
        Boolean metaHeader = Boolean.parseBoolean(config.getProperty("csv.has.header"));
        CsvFile.jsonize(config.getProperty("csv.file"),
                map, metaHeader, METADATA, metaRegex, redisQueue);
        LOG.info("Movies data has been added to the redis queue");
    }


    protected void stopWorkers() {
        for (RedisRunnable runnable: redisRunnables) {
            runnable.shutdown();
        }
        for (MongoRunnable runnable: mongoRunnables) {
            runnable.shutdown();
        }
        jsonFieldExecutor.shutdown();
    }

    public void shutdown() {
        stopWorkers();

        monitor.shutdown();
        shutdown.set(true);
    }

    class Monitor implements Runnable {

        @Override
        public void run() {
            final AtomicLong time = new AtomicLong(System.currentTimeMillis());
            while (!shutdown.get()) {
                if (System.currentTimeMillis() - time.get() >= 30000) {
                    time.set(System.currentTimeMillis());
                    System.out.print("\n****************************************\n"
                            + "\t\tCurrent queue counts:"
                            + "\n****************************************\n"
                            + "### Redis Queue ###\n"
                            + "Size: " + redisQueue.size() + "\n"
                     //       + "Metadata: " + metaProducerQueue.size() + "\n"
                            + "### Mongo Queue ###\n"
                            + "Size: " + mongoQueue.size() + "\n"
                     //       + "Metadata: " + metaQueue.size() + "\n"
                            + "## Redis Keys Written ###\n"
                            + "\n****************************************\n");
                    time.set(System.currentTimeMillis());
                }
            }
        }
    }
}
