package com.codality.data.tools.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.FileWriter

import com.codality.data.tools.proto.ParserConfigMessage.ParserConfig

internal class YamlPropertiesTest {

    private val LOG: Logger = LoggerFactory.getLogger(YamlPropertiesTest::class.java)

    @Test
    fun testLoad() {
    }

    @Test
    fun testStoreDefault() {
        val yamlProperties = YamlProperties()
        val file = kotlin.io.path.createTempFile("yaml_properties_test", ".yml").toFile()
        val writer = FileWriter(file)
        yamlProperties.saveDefault(writer)
        val processorConf = yamlProperties.load(file)
        assertEquals("localhost", processorConf.db!!.mongo!!.host)
        assertEquals("default", processorConf.db!!.mongo!!.dbName)
        assertEquals(27017, processorConf.db!!.mongo!!.port)
        assertEquals(6379, processorConf.db!!.redis!!.port)
        assertEquals("id", processorConf.format!!.csv!!.fieldsList[0].name)
        assertEquals(0, processorConf.format!!.csv!!.fieldsList[0].sourceColumn)
        assertEquals(1, processorConf.format!!.csv!!.fieldsList[1].destinationColumn)
    }
}