package com.codality.data.tools.parser.serialize

import com.codality.data.tools.Utils
import com.codality.data.tools.Utils.createJsonObject
import com.codality.data.tools.Utils.marshallJsonSequenceToJsonArray
import com.codality.data.tools.parser.Serializer
import com.codality.data.tools.proto.ParserConfigMessage
import com.google.gson.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.InputStreamReader

class Json(val config: ParserConfigMessage.ParserConfig): Serializer {

    private val parseNested = config.format.json.parseNested

    private fun gsonParseJson(inputStream: InputStream): JsonElement {
        val jsonParser = JsonStreamParser(InputStreamReader(inputStream))
        val parsedJson = jsonParser.asSequence()
        return marshallJsonSequenceToJsonArray(parsedJson)
    }

    override fun parse(inputStream: InputStream): JsonElement {
        val json = Utils.deserializeJsonFile(inputStream)
        return if (json.isJsonObject) {
            if (parseNested)
                parseNestedObjects(json.asJsonObject)
            else
                parseObject(json.asJsonObject)
        }
        else if (json.isJsonArray)
            json.asJsonArray.fold(JsonArray()) { acc, element ->
                if (element.isJsonObject)
                    if (parseNested)
                        acc.add(parseNestedObjects(element.asJsonObject))
                    else
                        acc.add(parseObject(element.asJsonObject))
                else if (element.isJsonArray && element.asJsonArray.size() == 1)
                    acc.add(parse(element.toString().byteInputStream()))
                else if (element.isJsonArray && element.asJsonArray.isEmpty)
                    acc.add(JsonNull.INSTANCE)
                else
                    acc.add(element)
                acc
            }
        else
            createJsonObject("value", json)
    }

    private fun parseObject(jsonObject: JsonObject): JsonObject {
        return checkTopLevelObject(jsonObject).entrySet().fold(JsonObject()) { jsonResult, element ->
            jsonResult.add(element.key, element.value)
            jsonResult
        }
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
                if (element.value.isJsonObject)
                    jsonResult.add(element.key, parseNestedObjects(element.value.asJsonObject))
                else
                    jsonResult.add(element.key, element.value)
                jsonResult
            }

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

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(Json::class.java)
    }
}