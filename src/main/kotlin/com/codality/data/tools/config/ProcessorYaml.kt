package com.codality.data.tools.config

data class ProcessorYaml (
    val format: FormatConf,
    val db: DbConf
)

data class CsvConf (
    val regex: String? = null,
    val fields: List<CsvField>? = null,
    val hasHeader: Boolean? = null,
)

class CsvField (
    val sourceColumn: Int? = null,
    val destinationColumn: Int? = null,
    val name: String? = null
)

data class DbConf (
    val mongo: MongoConf? = null,
    val redis: RedisConf? = null
)

data class FormatConf (
    val csv: CsvConf? = null,
    val json: JsonConf? = null
)

data class JsonConf (
    val parseNested: Boolean? = null
)

class MongoConf (
    val host: String? = null,
    val port: Int? = null,
    val dbName: String? = null,
    val workerCount: Int? = null
)

class RedisConf (
    val host: String? = null,
    val port: Int? = null,
    val workerCount: Int? = null
)