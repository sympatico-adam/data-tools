package com.codality.data.tools.config

import com.codality.data.tools.proto.ParserConfigMessage
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.io.File
import java.io.FileWriter


class ParserConf {

    private val gson: Gson = GsonBuilder()
        .registerTypeAdapter(
            ParserConfigMessage.ParserConfig::class.java,
            ParserConfigAdapter()
        ).create()


    fun load(file: File): ParserConfigMessage.ParserConfig {
        val processorYamlObj: Any = ObjectMapper(YAMLFactory()).readValue(file, Any::class.java)
        val processorYamlJson = gson.toJson(processorYamlObj)
        return gson.fromJson(processorYamlJson, ParserConfigMessage.ParserConfig::class.java)
    }

    fun save(parserConfig: ParserConfigMessage.ParserConfig, path: String = "./conf") {
        val jsonNodeTree = ObjectMapper().readTree(gson.toJson(parserConfig))
        val mapper = ObjectMapper(YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        mapper.writeValue(FileWriter(File("$path/parser_properties.yml")), jsonNodeTree)
    }

    fun saveDefault(path: String = "./conf") {
        val jsonNodeTree = ObjectMapper().readTree(gson.toJson(ParserConfigMessage.ParserConfig.getDefaultInstance()))
        val mapper = ObjectMapper(YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        mapper.writeValue(FileWriter(File("$path/default_parser_properties.yml")), jsonNodeTree)
    }

}