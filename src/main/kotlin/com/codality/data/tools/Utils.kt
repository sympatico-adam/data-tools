package com.codality.data.tools

import com.google.gson.*
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileReader
import java.io.InputStream
import java.io.InputStreamReader
import java.text.SimpleDateFormat
import kotlin.text.Charsets.UTF_8
import org.yaml.snakeyaml.reader.UnicodeReader

object Utils {

    private val gsonBuilder: Gson = GsonBuilder()
        //.enableComplexMapKeySerialization()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .setDateFormat(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").toLocalizedPattern())
        .setFieldNamingStrategy { field ->
            field?.name?.replace(".", "_")
        }
        .enableComplexMapKeySerialization()
        .setPrettyPrinting()
        .create()

    fun deserializeJsonObject(jsonElement: JsonElement): JsonObject {
        return gsonBuilder.fromJson(jsonElement, JsonObject::class.java)
    }

    fun deserializeJsonArray(jsonElement: JsonElement): JsonArray {
        return gsonBuilder.fromJson(jsonElement, JsonArray::class.java)
    }

    fun deserializeJsonFile(inputStream: InputStream): JsonElement {
        val jsonReader = gsonBuilder.newJsonReader(InputStreamReader(inputStream))
        return parseJsonReader(jsonReader)
    }

    fun deserializeJsonString(jsonString: String): JsonElement {
        return deserializeJsonByteArray(jsonString.toByteArray(UTF_8))
    }

    fun deserializeJsonByteArray(byteArray: ByteArray): JsonElement {
        val jsonReader = gsonBuilder.newJsonReader(UnicodeReader(ByteArrayInputStream(byteArray)).buffered())
        return parseJsonReader(jsonReader)
    }

    fun marshallJsonSequenceToJsonArray(jsonSequence: Sequence<JsonElement>): JsonArray {
        return jsonSequence.fold(JsonArray()) { acc, json ->
            acc.add(json)
            acc
        }
    }

    private fun parseJsonReader(jsonReader: JsonReader): JsonElement {
        return if (jsonReader.peek() == JsonToken.BEGIN_ARRAY)
            gsonBuilder.fromJson(jsonReader, JsonArray::class.java)
        else if (jsonReader.peek() == JsonToken.BEGIN_OBJECT)
            gsonBuilder.fromJson(jsonReader, JsonObject::class.java)
        else
            gsonBuilder.fromJson(jsonReader, JsonElement::class.java)
    }
    fun jsonToMap(json: JsonElement): Map<String, Map<String, Any>> {
        val hashMap: HashMap<String, HashMap<String, HashMap<String, Any>>> = hashMapOf()
        return gsonBuilder.fromJson(json, hashMap.javaClass)
    }

    fun createJsonObject(key: String, jsonElement: JsonElement): JsonObject {
        val json = JsonObject()
        json.add(key, jsonElement)
        return gsonBuilder.fromJson(json, JsonObject::class.java)
    }

}