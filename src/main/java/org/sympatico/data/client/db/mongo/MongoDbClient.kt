package org.sympatico.data.client.db.mongo

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import org.bson.codecs.*
import org.bson.codecs.configuration.CodecRegistries
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

class MongoDbClient (val host: String, val port: Int) {

    private var mongoClient: MongoClient

    init {
        val codecRegistry = CodecRegistries.fromCodecs(
            BsonArrayCodec(),
            DocumentCodec(),
            BsonStringCodec(),
            StringCodec(),
            LongCodec(),
            IntegerCodec(),
            MapCodec(),
            BsonBooleanCodec(),
            BsonDocumentCodec(),
            PatternCodec()
        )
        val mongoClientSettings = MongoClientSettings.builder()
            .applyConnectionString(
                ConnectionString("mongodb://$host:$port")
            )
            .codecRegistry(codecRegistry)
            .build()
        mongoClient = MongoClients.create(mongoClientSettings)
        connected.set(true)
    }

    fun getDatabase(database: String): MongoDatabase {
        return mongoClient.getDatabase(database)
    }

    @Synchronized
    fun shutdown() {
        LOG.info("shutting down mongo client...")
        if (connected.getAndSet(false)) mongoClient.close()
    }

    companion object {
        private val connected = AtomicBoolean(false)
        private val LOG = LoggerFactory.getLogger(MongoDbClient::class.java)
    }
}