package com.codality.data.tools.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import kotlin.test.assertTrue

internal class ParserConfTest {

    private val LOG: Logger = LoggerFactory.getLogger(ParserConfTest::class.java)

    @Test
    fun testSSaveDefault() {
        val parserConf = ParserConf()
        parserConf.saveDefault()
        val file = File("./conf")
        assertTrue(file.exists())
        file.delete()
    }

    @Test
    fun testSSave() {
        val file = File(ParserConfTest::class.java.classLoader.getResource("csv-metadata.yml")!!.toURI())
        val parserConfig = ParserConf().load(file)
        val newFile = File("./conf")
        ParserConf().save(parserConfig, newFile.path)
        newFile.delete()
    }

    @Test
    fun testLoad() {
        val file = File(ParserConfTest::class.java.classLoader.getResource("csv-metadata.yml")!!.toURI())
        val parserConf = ParserConf().load(file)
        assertEquals("localhost", parserConf.db!!.mongo!!.host)
        assertEquals("default", parserConf.db!!.mongo!!.dbName)
        assertEquals(27017, parserConf.db!!.mongo!!.port)
        assertEquals(6379, parserConf.db!!.redis!!.port)
        assertEquals("id", parserConf.format!!.csv!!.fieldsList[0].name)
        assertEquals(5, parserConf.format!!.csv!!.fieldsList[0].sourceColumn)
        assertEquals(1, parserConf.format!!.csv!!.fieldsList[1].destinationColumn)
    }
}