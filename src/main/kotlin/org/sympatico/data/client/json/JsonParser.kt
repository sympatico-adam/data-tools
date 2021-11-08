package org.sympatico.data.client.json

import com.google.gson.*
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.*
import java.text.SimpleDateFormat
import java.util.*
import kotlin.collections.HashMap

class JsonParser {

    private val LOG: Logger = LoggerFactory.getLogger(JsonParser::class.java)

    private val gsonBuilder: Gson = GsonBuilder()
        //.enableComplexMapKeySerialization()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .setDateFormat(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").toLocalizedPattern())
        .setFieldNamingStrategy { field ->
            field?.name?.replace(".", "_")
        }
        .create()

    fun parseJsonFile(file: File): Pair<String, ByteArray> {
        LOG.info("Streaming json file:\n${file.path}\n")
        val json = deserializeJsonFile(file)
        LOG.info("normalizing json structure:\n$json\n")
        val result = parseJson(json)
        LOG.info("*** finished parsing\n" +
                 "*** ${file.name} " +
                "***********************" +
                "\n${result}\n")
        return file.nameWithoutExtension to result.toString().toByteArray()
    }

    fun parseJson(jsonElement: JsonElement): JsonElement {
        return cleanJsonFieldNames(jsonElement)
    }

    fun parseJson(key: String, jsonElement: JsonElement): List<JsonElement> {
        LOG.info("normalizing json structure for key [ $key ]:\n$jsonElement\n")
        val jsonList = mutableListOf<JsonElement>()
        if (jsonElement.isJsonArray) {
            val jsonArray = JsonArray()
            jsonElement.asJsonArray.forEach { element ->
                if (element.isJsonObject)
                    jsonArray.add(deserializeJsonObject(element))
                else if (element.isJsonPrimitive)
                    jsonArray.add(jsonElement)
            }
        } else if (jsonElement.isJsonObject)
            jsonList.add(cleanJsonFieldNames(jsonElement.asJsonObject))
        else if (jsonElement.isJsonPrimitive)
            jsonList.add(createJsonObject(UUID.randomUUID().toString(), jsonElement))
        LOG.info("finished parsing json key [ $key ]:\n$jsonList\n")
        return jsonList
    }

    fun deserializeJsonObject(jsonElement: JsonElement): JsonObject {
        return cleanJsonFieldNames(gsonBuilder.fromJson(jsonElement, JsonObject::class.java)).asJsonObject
    }

    fun deserializeJsonArray(jsonElement: JsonElement): JsonArray {
        return cleanJsonFieldNames(gsonBuilder.fromJson(jsonElement, JsonArray::class.java)).asJsonArray
    }

    fun accumulateJsonObject(acc: JsonObject, jsonObject: JsonObject): JsonObject {
        jsonObject.asJsonObject.entrySet().forEach { element ->
            if (acc.has(element.key) && acc[element.key].isJsonArray)
                acc[element.key].asJsonArray.add(element.value)
            else if (acc.has(element.key) && acc[element.key].isJsonObject) {
                val arrayElement = JsonArray()
                arrayElement.add(acc[element.key]).let { arrayElement.add(element.value) }
                acc.add(element.key, arrayElement)
            } else
                acc.add(element.key, element.value)
        }
        return acc
    }

    private fun deserializeJsonFile(file: File): JsonElement {
        val jsonReader = gsonBuilder.newJsonReader(FileReader(file))
        return cleanJsonFieldNames(parseJsonReader(jsonReader))
    }

    fun deserializeJsonByteArray(byteArray: ByteArray): JsonElement {
        val jsonReader = gsonBuilder.newJsonReader(InputStreamReader(ByteArrayInputStream(byteArray)))
        return cleanJsonFieldNames(parseJsonReader(jsonReader))
    }

    private fun parseJsonReader(jsonReader: JsonReader): JsonElement {
        return if (jsonReader.peek() == JsonToken.BEGIN_ARRAY)
            gsonBuilder.fromJson(jsonReader, JsonArray::class.java)
        else if (jsonReader.peek() == JsonToken.BEGIN_OBJECT)
            gsonBuilder.fromJson(jsonReader, JsonObject::class.java)
        else
            gsonBuilder.fromJson(jsonReader, JsonElement::class.java)
    }

    private fun createJsonObject(key: String, jsonElement: JsonElement): JsonObject {
        val json = JsonObject()
        json.add(key.replace(".", "_"), jsonElement)
        return gsonBuilder.fromJson(json, JsonObject::class.java)
    }

    fun jsonToMap(json: JsonElement): Map<String, Map<String, Any>> {
        val hashMap: HashMap<String, HashMap<String, HashMap<String, Any>>> = hashMapOf()
        return gsonBuilder.fromJson(json, hashMap.javaClass)
    }

    private fun cleanJsonFieldNames(jsonElement: JsonElement): JsonElement {
        return if (jsonElement.isJsonObject)
            cleanJsonObjectFieldNames(jsonElement.asJsonObject)
        else if (jsonElement.isJsonArray)
            jsonElement.asJsonArray.fold(JsonArray()) { acc, element ->
                if (element.isJsonObject)
                    acc.add(cleanJsonObjectFieldNames(element.asJsonObject))
                else if (element.isJsonArray && !element.asJsonArray.isEmpty)
                    acc.add(JsonNull.INSTANCE)
                else
                    acc.add(element)
                acc
            }
        else jsonElement
    }

    private fun cleanJsonObjectFieldNames(jsonObject: JsonObject): JsonObject {
        return jsonObject.asJsonObject.entrySet().fold(JsonObject()) { acc, element ->
            acc.add(element.key.replace(".", "_"), element.value)
            acc
        }
    }

}