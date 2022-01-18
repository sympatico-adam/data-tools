package com.codality.data.tools.parser

import com.google.gson.JsonElement
import java.io.InputStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory

interface Serializer {

    fun parse(inputStream: InputStream): JsonElement

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(Serializer::class.java)
    }

}