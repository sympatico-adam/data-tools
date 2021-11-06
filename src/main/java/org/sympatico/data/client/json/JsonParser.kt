package org.sympatico.data.client.json

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.json.JsonGeneratorImpl
import com.fasterxml.jackson.core.json.WriterBasedJsonGenerator
import com.fasterxml.jackson.core.util.JsonGeneratorDelegate
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.sympatico.data.client.file.CsvFileClient
import java.io.*

class JsonParser() {

    private val LOG: Logger = LoggerFactory.getLogger(CsvFileClient::class.java)

    fun parseJsonFile(file: File): Pair<String, String> {
        LOG.info("Streaming json file: ${file.path}")
        val objectMapper = ObjectMapper()
        val json = objectMapper.readTree(file)
        val normalized = objectMapper.readTree(normalizeFieldNames(json, objectMapper))
        LOG.info("Processed ${file.name}: \n${normalized}")
        return file.nameWithoutExtension to normalized.asText()
    }

    fun normalizeFieldNames(jsonNode: JsonNode, objectMapper: ObjectMapper): String {
        val parser = jsonNode.traverse()
        val writer = StringWriter()
        val generator = objectMapper.factory.createGenerator(writer)
        val sequenceWriter = objectMapper.writerWithDefaultPrettyPrinter()
            .withoutRootName()
            .with(objectMapper.factory)
            .writeValues(generator)
        sequenceWriter.init(false)
        parser.nextToken()
        generator.copyCurrentStructure(parser)
        parser.clearCurrentToken()
        while (parser.hasCurrentToken()) {
            parser.clearCurrentToken()
            val name = parser.nextFieldName()
            if (name != null && name.contains(Regex("\\."))) {
                LOG.info("replacing invalid field name: $name")
                parser.overrideCurrentName(name.replace(Regex("\\."), "_"))
            }
            generator.writeRaw(parser.text)

        }
        parser.finishToken()
        sequenceWriter.close()
        generator.close()
        writer.close()
        return writer.buffer.toString()
    }
}