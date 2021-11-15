package com.codality.data.tools.config

import com.codality.data.tools.proto.ParserConfigMessage.ParserConfig
import com.google.gson.JsonParser
import com.google.gson.TypeAdapter
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import com.google.protobuf.util.JsonFormat
import java.io.IOException

class ParserConfigAdapter : TypeAdapter<ParserConfig>() {
    /**
     * Override the read method to return a {@Person} object from it's json representation.
     */
    @Throws(IOException::class)
    override fun read(jsonReader: JsonReader): ParserConfig {
        // Create a builder for the ParserConfig message
        val parserConfigBuilder: ParserConfig.Builder = ParserConfig.newBuilder()
        // Use the JsonFormat class to parse the json string into the builder object
        // The Json string will be parsed fromm the JsonReader object
        JsonFormat.parser().merge(JsonParser.parseReader(jsonReader).toString(), parserConfigBuilder)
        // Return the built ParserConfig message
        return parserConfigBuilder.build()
    }

    /**
     * Override the write method and set the json value of the Person message.
     */
    @Throws(IOException::class)
    override fun write(jsonWriter: JsonWriter, config: ParserConfig?) {
        // Call the printer of the JsonFormat class to convert the Person proto message to Json
        jsonWriter.jsonValue(JsonFormat.printer().print(config))
    }
}