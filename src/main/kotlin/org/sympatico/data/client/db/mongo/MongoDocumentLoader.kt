package org.sympatico.data.client.db.mongo

import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.ExperimentalTime

class MongoDocumentLoader (host: String, port: Int) {

    private val queue: ConcurrentLinkedQueue<Pair<String, ByteArray>> = ConcurrentLinkedQueue<Pair<String, ByteArray>>()
    private var executorService: ExecutorService = Executors.newCachedThreadPool()
    private var runnables: MutableList<MongoRunnable> = mutableListOf()
    private val mongoDbClient = MongoDbClient(host, port)

    fun startMongoDocumentLoader(workerCount: Int, database: String) {
        for (i in 0 until workerCount) {
            runnables.add(MongoRunnable(mongoDbClient.getDatabase(database), queue))
            executorService.submit(runnables[i])
        }
    }

    fun getRunnableQueue(): ConcurrentLinkedQueue <Pair<String, ByteArray>> {
        return queue
    }

    @ExperimentalTime
    @Synchronized
    fun shutdown() {
        LOG.info("shutting down mongo loader...")
        if (!executorService.isShutdown) for (runnable in runnables) {
            runnable.shutdown()
        }
        executorService.shutdown()
        mongoDbClient.shutdown()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MongoDocumentLoader::class.java)
    }
}