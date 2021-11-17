package com.codality.data.tools.db.mongo

import com.codality.data.tools.proto.ParserConfigMessage
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.time.ExperimentalTime

class MongoDocumentLoader(
    private val config: ParserConfigMessage.ParserConfig,
    private val queue: ConcurrentLinkedQueue<Pair<String, ByteArray>>
    ) {

    private var executorService: ExecutorService = Executors.newCachedThreadPool()
    private var runnables: MutableList<MongoRunnable> = mutableListOf()
    val mongoClient = MongoDbClient(config.db.mongo.host, config.db.mongo.port)

    fun startMongoDocumentLoader() {
        val workerCount = config.db.mongo.workerCount
        val database = config.db.mongo.dbName
        for (i in 0 until workerCount) {
            runnables.add(MongoRunnable(mongoClient.getDatabase(database), queue))
            executorService.submit(runnables[i])
        }
    }

    fun getRunnableQueue(): ConcurrentLinkedQueue<Pair<String, ByteArray>> {
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
        mongoClient.shutdown()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MongoDocumentLoader::class.java)
    }
}