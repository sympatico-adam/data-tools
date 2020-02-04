package org.sympatico.data.client.db;

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DbClientRunner {

    private DbClient dbClient;
    private ExecutorService executorService;
    private DbClientRunnable[] runnables;
    private ConcurrentLinkedQueue<Pair<String, byte[]>> queue;

    public DbClientRunner(DbClient dbClient, ConcurrentLinkedQueue<Pair<String, byte[]>> queue) {
        this.dbClient = dbClient;
        this.queue = queue;
        final DbClientRunner daemonRef = this;
        Runtime.getRuntime().addShutdownHook(new Thread(daemonRef::shutdown));
    }

    public void run(int workerCount) {
        executorService = Executors.newFixedThreadPool(workerCount);
        runnables = new DbClientRunnable[workerCount];
        for (int i = 0; i < workerCount; i++) {
            runnables[i] = new DbClientRunnable(dbClient, queue);
            executorService.submit(runnables[i]);
        }
    }

    private synchronized void shutdown() {
        if (!executorService.isShutdown())
            for (DbClientRunnable runnable: runnables) {
                runnable.shutdown();
            }
            executorService.shutdown();
    }
}
