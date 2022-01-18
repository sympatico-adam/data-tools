package com.codality.data.tools.db.client

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import java.util.concurrent.atomic.AtomicBoolean
import org.bson.codecs.BsonArrayCodec
import org.bson.codecs.BsonBooleanCodec
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonStringCodec
import org.bson.codecs.DocumentCodec
import org.bson.codecs.IntegerCodec
import org.bson.codecs.LongCodec
import org.bson.codecs.MapCodec
import org.bson.codecs.PatternCodec
import org.bson.codecs.StringCodec
import org.bson.codecs.configuration.CodecRegistries
import org.slf4j.LoggerFactory

class Mongo(
    val host: String,
    val port: Int
) {

    var mongoClient: MongoClient

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
        private val LOG = LoggerFactory.getLogger(Mongo::class.java)
    }
}