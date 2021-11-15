package com.codality.data.tools

import com.google.gson.*
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileReader
import java.io.InputStreamReader
import java.text.SimpleDateFormat

object Utils {

    private val gsonBuilder: Gson = GsonBuilder()
        //.enableComplexMapKeySerialization()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .setDateFormat(SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").toLocalizedPattern())
        .setFieldNamingStrategy { field ->
            field?.name?.replace(".", "_")
        }
        .create()

    fun deserializeJsonObject(jsonElement: JsonElement): JsonObject {
        return parseNestedObjects(gsonBuilder.fromJson(jsonElement, JsonObject::class.java)).asJsonObject
    }

    fun deserializeJsonArray(jsonElement: JsonElement): JsonArray {
        return parseNestedElements(gsonBuilder.fromJson(jsonElement, JsonArray::class.java)).asJsonArray
    }

    fun parseNestedElements(jsonElement: JsonElement): JsonElement {
        return if (jsonElement.isJsonObject)
            parseNestedObjects(jsonElement.asJsonObject)
        else if (jsonElement.isJsonArray)
            jsonElement.asJsonArray.fold(JsonArray()) { acc, element ->
                if (element.isJsonObject)
                    acc.add(parseNestedObjects(element.asJsonObject))
                else if (element.isJsonArray && element.asJsonArray.isEmpty)
                    acc.add(JsonNull.INSTANCE)
                else
                    acc.add(element)
                acc
            }
        else
            jsonElement
    }

    fun parseElements(jsonElement: JsonElement): JsonElement {
        return if (jsonElement.isJsonObject) {
            parseObject(jsonElement.asJsonObject)
        } else if (jsonElement.isJsonArray) {
            jsonElement.asJsonArray.fold(JsonArray()) { acc, element ->
                if (element.isJsonObject)
                    acc.add(parseObject(element.asJsonObject))
                else if (element.isJsonArray && element.asJsonArray.isEmpty)
                    acc.add(JsonNull.INSTANCE)
                else
                    acc.add(element)
                acc
            }
        } else jsonElement
    }

    private fun parseNestedObjects(jsonObject: JsonObject): JsonObject {
        return checkTopLevelObject(jsonObject).entrySet().fold(JsonObject()) { jsonResult, element ->
            if (element.key.contains('.')) {
                val hierarchy = element.key.split('.')
                val subKey = hierarchy.takeLast(2)[0]
                val existingMembers = if (jsonResult.has(element.key))
                    getNestedHierarchy(jsonResult.get(element.key).asJsonObject, element.key)
                else emptyList()
                if (existingMembers.isNotEmpty() && existingMembers.first() == hierarchy.first())
                    jsonResult[subKey].asJsonObject
                else
                    JsonObject()
                hierarchy.foldRight(JsonObject()) { field, acc ->
                    val nested = JsonObject()
                    if (field == hierarchy.last()) {
                        existingMembers.fold(jsonObject) { a, f ->
                            if (f == field && a.get(f).isJsonObject)
                                a.get(f).asJsonObject.entrySet().forEach { e ->
                                    nested.add(e.key, e.value)
                                }
                            a
                        }
                        nested.add(field, element.value)
                    } else if (field == hierarchy.first()) {
                        nested.add(field, acc)
                        mergeJsonObjects(jsonResult, nested)
                    } else
                        nested.add(field, acc)
                    nested
                }
                jsonResult
            } else {
                jsonResult.add(element.key, element.value)
                jsonResult
            }

        }
    }

    private fun parseObject(jsonObject: JsonObject): JsonObject {
        return checkTopLevelObject(jsonObject).entrySet().fold(JsonObject()) { jsonResult, element ->
            jsonResult.add(element.key, element.value)
            jsonResult
        }
    }

    private fun mergeJsonObjects(acc: JsonObject, jsonObject: JsonObject): JsonElement {
        jsonObject.asJsonObject.entrySet().forEach { element ->
            if (acc.has(element.key) && acc[element.key].isJsonArray)
                acc[element.key].asJsonArray.forEachIndexed { idx, member ->
                    if (member.isJsonObject)
                        acc.add(
                            element.key,
                            mergeJsonObjects(
                                acc[element.key].asJsonArray[idx].asJsonObject,
                                member.asJsonObject
                            )
                        )
                } else if (acc.has(element.key) && acc[element.key].isJsonObject) {
                mergeJsonObjects(acc.get(element.key).asJsonObject, element.value.asJsonObject)
            } else {
                val newKey = if (acc.has(element.key)) "${element.key}_NEW"
                else element.key
                acc.add(newKey, element.value)
            }
        }
        return acc
    }

    private fun checkTopLevelObject(jsonObject: JsonObject): JsonObject {
        val objectEntries = jsonObject.asJsonObject.entrySet()
        return if (objectEntries.size == 1 &&
            objectEntries.first().value.isJsonObject)
            objectEntries.first().value.asJsonObject
        else jsonObject
    }

    private fun getNestedHierarchy(jsonObject: JsonObject, startKey: String? = null): List<String> {
        return if (startKey != null && jsonObject.get(startKey).isJsonObject)
            jsonObject.get(startKey).asJsonObject.entrySet().fold(mutableListOf<String>()) { hierarchy, element ->
                if (jsonObject.get(element.key).isJsonObject) {
                    hierarchy.add(element.key)
                    hierarchy.addAll(getNestedHierarchy(element.value.asJsonObject, element.key))
                }
                hierarchy
            } else emptyList()
    }

    fun deserializeJsonFile(file: File): JsonElement {
        val jsonReader = gsonBuilder.newJsonReader(FileReader(file))
        return parseNestedElements(parseJsonReader(jsonReader))
    }

    fun deserializeJsonString(jsonString: String): JsonElement {
        return parseNestedElements(deserializeJsonByteArray(jsonString.toByteArray()))
    }

    fun deserializeJsonByteArray(byteArray: ByteArray): JsonElement {
        val jsonReader = gsonBuilder.newJsonReader(InputStreamReader(ByteArrayInputStream(byteArray)))
        return parseNestedElements(parseJsonReader(jsonReader))
    }

    fun createJsonObject(key: String, jsonElement: JsonElement): JsonObject {
        val json = JsonObject()
        json.add(key.replace(".", "_"), jsonElement)
        return gsonBuilder.fromJson(json, JsonObject::class.java)
    }

    fun jsonToMap(json: JsonElement): Map<String, Map<String, Any>> {
        val hashMap: HashMap<String, HashMap<String, HashMap<String, Any>>> = hashMapOf()
        return gsonBuilder.fromJson(json, hashMap.javaClass)
    }

    private fun parseJsonReader(jsonReader: JsonReader): JsonElement {
        return if (jsonReader.peek() == JsonToken.BEGIN_ARRAY)
            gsonBuilder.fromJson(jsonReader, JsonArray::class.java)
        else if (jsonReader.peek() == JsonToken.BEGIN_OBJECT)
            gsonBuilder.fromJson(jsonReader, JsonObject::class.java)
        else
            gsonBuilder.fromJson(jsonReader, JsonElement::class.java)
    }

}