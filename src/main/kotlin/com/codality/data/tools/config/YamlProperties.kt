package com.codality.data.tools.config

import com.codality.data.tools.proto.ParserConfigMessage
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator
import java.io.File
import java.io.Writer

class YamlProperties {

    private val jsonConf = JsonConf(parseNested = true)
    private val csvFields: List<CsvField> = listOf(
        CsvField(sourceColumn = 0, destinationColumn = 0, name = "id"),
        CsvField(sourceColumn = 1, destinationColumn = 1, name = "name")
    )
    private val csvConf = CsvConf(regex = ",", fields = csvFields, hasHeader = true)
    private val formatConf = FormatConf(csvConf, jsonConf)
    private val mongoConf = MongoConf(host = "localhost", port = 27017, dbName = "default", workerCount = 1)
    private val redisConf = RedisConf(host = "localhost", port = 6379, workerCount = 1)
    private val dbConf = DbConf(redis = redisConf, mongo = mongoConf)
    private val defaultConf = ProcessorYaml(db = dbConf, format = formatConf)

    fun load(file: File): ParserConfigMessage.ParserConfig {
        val mapper = ObjectMapper(YAMLFactory())
        mapper.findAndRegisterModules()
        val processorYaml: ProcessorYaml = mapper.readValue(file, ProcessorYaml::class.java)
        val mongoConf = ParserConfigMessage.MongoConf.newBuilder()
                .setDbName(processorYaml.db.mongo?.dbName)
                .setHost(processorYaml.db.mongo?.host)
                .setPort(processorYaml.db.mongo?.port!!)
                .setWorkerCount(processorYaml.db.mongo.workerCount!!)
            .build()
        val redisConf = ParserConfigMessage.RedisConf.newBuilder()
            .setHost(processorYaml.db.redis?.host)
            .setPort(processorYaml.db.redis?.port!!)
            .setWorkerCount(processorYaml.db.redis.workerCount!!)
            .build()
        val jsonConf = ParserConfigMessage.JsonConf.newBuilder()
            .setParseNested(processorYaml.format.json?.parseNested!!)
            .build()
        val csvConf = ParserConfigMessage.CsvConf.newBuilder()
            .setHasHeader(processorYaml.format.csv?.hasHeader!!)
            .setRegex(processorYaml.format.csv.regex)
            .addAllFields(processorYaml.format.csv.fields.orEmpty().map { field ->
                ParserConfigMessage.CsvField.newBuilder()
                    .setName(field.name)
                    .setSourceColumn(field.sourceColumn!!)
                    .setDestinationColumn(field.destinationColumn!!)
                    .build()
                }.toList()
            )
            .build()
        val formatConf = ParserConfigMessage.FormatConf.newBuilder()
            .setCsv(csvConf)
            .setJson(jsonConf)
            .build()
        val dbConf = ParserConfigMessage.DbConf.newBuilder()
            .setMongo(mongoConf)
            .setRedis(redisConf)
            .build()
        val parserConfig = ParserConfigMessage.ParserConfig.newBuilder()
            .setDb(dbConf)
            .setFormat(formatConf)
            .build()
        return parserConfig
    }

    fun saveDefault(writer: Writer?) {
        val mapper = ObjectMapper(YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        mapper.writeValue(writer, defaultConf);
    }

}