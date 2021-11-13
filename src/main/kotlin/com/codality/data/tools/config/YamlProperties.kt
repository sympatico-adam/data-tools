package com.codality.data.tools.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator
import java.io.File
import java.io.Writer


class YamlProperties {

    private val jsonConf = JsonConf(parseNested = true)
    private val csvFields: List<CsvField> = listOf(
        CsvField(sourceColumn = 0, targetColumn = 0, name = "id"),
        CsvField(sourceColumn = 1, targetColumn = 1, name = "name")
    )
    private val csvConf = CsvConf(regex = ",", fields = csvFields, hasHeader = true)
    private val formatConf = FormatConf(csvConf, jsonConf)
    private val mongoConf = MongoConf(host = "localhost", port = 27017, dbName = "default", workerCount = 1)
    private val redisConf = RedisConf(host = "localhost", port = 6379, workerCount = 1)
    private val dbConf = DbConf(redis = redisConf, mongo = mongoConf)

    private val defaultConf = ProcessorConf(db = dbConf, format = formatConf)

    fun load(file: File): ProcessorConf {
        val mapper = ObjectMapper(YAMLFactory())
        mapper.findAndRegisterModules()
        val processorConf: ProcessorConf = mapper.readValue(file, ProcessorConf::class.java)
        return processorConf
    }

    fun saveDefault(writer: Writer?) {
        val mapper = ObjectMapper(YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        mapper.writeValue(writer, defaultConf);
    }

}