package com.codality.data.tools.file

import com.codality.data.tools.parser.CsvParser
import com.codality.data.tools.config.ParserConf
import com.codality.data.tools.db.mongo.MongoDocumentLoader
import com.codality.data.tools.parser.FileParser
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.*
import java.util.regex.Pattern
import kotlin.time.ExperimentalTime
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach

class CsvLoaderTest {

    companion object {
        private val LOG = LoggerFactory.getLogger(CsvLoaderTest::class.java)
    }

    private val config =
        ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("mongo-config.yml")!!.file))
    lateinit var server: MongoServer

    @BeforeEach
    fun setup() {
        server = MongoServer(MemoryBackend())
        server.bind(config.db.mongo.host, config.db.mongo.port)
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Throws(IOException::class)
    @Test
    fun jsonizeBrokenCsv() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("csv-metadata.yml")!!.file))
        val parser = CsvParser(config)
        parser.parse(
            File(CsvLoaderTest::class.java.classLoader.getResource("movies_metadata_small_fixed.csv")!!.file),
            "movies_metadata"
        )
        assertEquals(19949, parser.getQueue().size)
    }

    @Throws(IOException::class)
    @Test
    fun jsonStandardizeCsv() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("csv-ratings.yml")!!.file))
        val parser = CsvParser(config)
        parser.parse(File(CsvLoaderTest::class.java.classLoader.getResource("ratings_small.csv")!!.file), "ratings_small")
    }

    @Test
    fun regex() {
        val str =
            """False,"{'id': 87536, 'name': 'Warlock Collection', 'poster_path': '/wkwtv5NMpBfo1eGjZpNJEQ2cZH0.jpg', 'backdrop_path': '/im5iJHo8UwswhKHlcADqUO3sZtA.jpg'}",7000000,"[{'id': 12, 'name': 'Adventure'}, {'id': 35, 'name': 'Comedy'}, {'id': 14, 'name': 'Fantasy'}, {'id': 27, 'name': 'Horror'}]",,11342,tt0098622,en,Warlock,"A warlock flees from the 17th to the 20th century, with a witch-hunter in hot pursuit. A Warlock (Julian Sands) is taken captive in Boston, Massachusetts in 1691 by a witch-hunter Giles Redferne (Richard Grant).  He is sentenced to death for his activities, including the bewitching of Redferne's bride-to-be, but before the execution a demon appears and propels the Warlock forward in time to 20th century Los Angeles, California. Redferne follows through the portal.
 The Warlock attempts to assemble The Grand Grimoire, a Satanic book that will reveal the ""true"" name of God.  Redferne and the Warlock then embark on a cat-and-mouse chase with the Grand Grimoire, and Kassandra (Lori Singer), a waitress who encounters Giles while he's attempting to find Warlock.",11.906872,/pEzCLGxq4bXafTORASwGtLKptLT.jpg,"[{'name': 'New World Pictures', 'id': 1950}]","[{'iso_3166_1': 'US', 'name': 'United States of America'}]",1989-06-01,0,103.0,"[{'iso_639_1': 'en', 'name': 'English'}]",Released,Satan also has one son.,Warlock,False,5.8,98"""
        val str2 =
            "False,\"{'id': 87536, 'name': 'Warlock Collection', 'poster_path': '/wkwtv5NMpBfo1eGjZpNJEQ2cZH0.jpg', 'backdrop_path': '/im5iJHo8UwswhKHlcADqUO3sZtA.jpg'}\",7000000"
        //Pattern splitter = Pattern.compile("^(True|False),(\"(\\[?\\{(?:(?:'[\\w\\d\\p{Punct}\\s]+':\\s'?['\\w\\d\\p{Punct}\\s]+'?)(?:,\\s)?)+'?}]?)\"|,\"([\\w\\d\\p{Punct}\\s&&[^\\[\\]{}]\n\r\t]+)\"|,([\\d]{1,9}.?[\\d]{0,9})|,([a-zA-Z\\s.]+)|,([\\w\\s]+))++$", Pattern.DOTALL);
        val splitter2 = Pattern.compile("""(?=[^\s]),(?=[^\s])""")
        //Matcher m = splitter.matcher(str);
        val split = splitter2.split(str)
        //System.out.LOG.info("Split str [" + split.length + "]: " + Arrays.toString(split));
        for (p in split) {
            LOG.info(p)
        }
    }

    @Throws(IOException::class)
    @Test
    fun testDelimiter() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("csv-metadata.yml")!!.file))
        val delimiter = config.format!!.csv!!.delimiterRegex
        val line =
            "False,,0,\"[{'id': 80, 'name': 'Crime'}, {'id': 18, 'name': 'Drama'}]\",,74295,tt0086199,fi,Rikos ja rangaistus,\"An adaptation of Dostoyevsky's novel, updated to present-day Helsinki. Slaughterhouse worker Rahikainen murders a man, and is forced to live with the consequences of his actions...\",1.473622,/aqu3HrpHaY8MR2ZOIfuUTWC3r3N.jpg,\"[{'name': 'Villealfa Filmproduction Oy', 'id': 2303}]\",\"[{'iso_3166_1': 'FI', 'name': 'Finland'}]\",1983-12-02,0,93.0,\"[{'iso_639_1': 'fi', 'name': 'suomi'}]\",Released,Crime and Punishment,Crime and Punishment,False,5.9,19"

        val splitLine = line.split(delimiter.toRegex()).toTypedArray()
        LOG.info("$splitLine\n")
        val json = JsonObject()
        json.add("id", JsonPrimitive(splitLine[5]))
        json.add("budget", JsonPrimitive(splitLine[2]))
        json.add("genre", JsonPrimitive(splitLine[3]))
        json.add("popularity", JsonPrimitive(splitLine[10]))
        json.add("company", JsonPrimitive(splitLine[12]))
        json.add("date", JsonPrimitive(splitLine[14]))
        json.add("revenue", JsonPrimitive(splitLine[15]))
        LOG.info(json.toString())
    }

    @ExperimentalTime
    fun parserTest() {
        val config = ParserConf().load(File(CsvLoaderTest::class.java.classLoader.getResource("csv-files.yml")!!.file))
        val parser = CsvParser(config)
        val loader = MongoDocumentLoader(config, parser.getQueue())
        loader.startMongoDocumentLoader()
        val files = FileParser.findFilesInPath("data/", "csv")
        files.forEach { file ->
            LOG.info("parsing test file: ${file.nameWithoutExtension}")
            parser.parse(file, file.nameWithoutExtension)
        }
        loader.shutdown()
    }

}