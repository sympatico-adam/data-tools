package org.sympatico.data.client.file;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

public class CsvFileTest {

    private final Properties config = new Properties();
    private static final Logger LOG  = LoggerFactory.getLogger(CsvFileTest.class);

    @Before
    public void setup() throws Exception {
        config.load(Objects.requireNonNull(CsvFileTest.class.getClassLoader().getResourceAsStream("client.test.properties")));
    }

    @Test
    public void jsonizeBrokenCsv() throws IOException {
        Map<String, Integer> map = new HashMap<>();
        map.put("id", 5);
        map.put("title", 20);
        map.put("budget", 2);
        map.put("genres", 3);
        map.put("popularity", 10);
        map.put("companies", 12);
        map.put("date", 14);
        map.put("revenue", 15);
        String regex = config.getProperty("csv.regex");
        String inputFilePath = config.getProperty("csv.file");
        File tempFile = File.createTempFile("test-csv-file", ",tmp");
        File outFile = File.createTempFile("test-csv-file", ",tmp");
        tempFile.deleteOnExit();
        outFile.deleteOnExit();
        long normalizedLineCount = CsvFile.writeNormalizedFile(inputFilePath, tempFile.getAbsolutePath());
        long parsedLineCount = CsvFile.jsonize(tempFile.getAbsolutePath(), map, regex, outFile.getAbsolutePath());
        Assert.assertEquals(normalizedLineCount, parsedLineCount);
        long actualCount = 0L;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader((new FileInputStream(outFile))))) {
            while (bufferedReader.readLine() != null) {
                actualCount++;
            }
        }
        Assert.assertEquals(parsedLineCount, actualCount);
    }

    @Test
    public void jsonStandardizeCsv() throws IOException {
        Map<String, Integer> map = new HashMap<>();
        map.put("id", 1);
        map.put("rating", 2);
        String inputFilePath = "ratings_small.csv";
        File tempFile = File.createTempFile("test-csv-file", ",tmp");
        File outFile = File.createTempFile("test-csv-file", ",tmp");
        tempFile.deleteOnExit();
        outFile.deleteOnExit();
        long normalizedLineCount =
                CsvFile.writeNormalizedFile(
                        Objects.requireNonNull(CsvFileTest.class.getClassLoader().getResource(inputFilePath)).getPath(),
                        tempFile.getAbsolutePath());
        long parsedLineCount = CsvFile.jsonize(tempFile.getAbsolutePath(), map, ",", outFile.getAbsolutePath());
        Assert.assertEquals(normalizedLineCount, parsedLineCount);
        long actualCount = 0L;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader((new FileInputStream(outFile))))) {
            while (bufferedReader.readLine() != null) {
                actualCount++;
            }
        }
        Assert.assertEquals(parsedLineCount, actualCount);
    }

    @Test
    public void regex() {
        String str = "False,\"{'id': 87536, 'name': 'Warlock Collection', 'poster_path': '/wkwtv5NMpBfo1eGjZpNJEQ2cZH0.jpg', 'backdrop_path': '/im5iJHo8UwswhKHlcADqUO3sZtA.jpg'}\",7000000,\"[{'id': 12, 'name': 'Adventure'}, {'id': 35, 'name': 'Comedy'}, {'id': 14, 'name': 'Fantasy'}, {'id': 27, 'name': 'Horror'}]\",,11342,tt0098622,en,Warlock,\"A warlock flees from the 17th to the 20th century, with a witch-hunter in hot pursuit. A Warlock (Julian Sands) is taken captive in Boston, Massachusetts in 1691 by a witch-hunter Giles Redferne (Richard Grant).  He is sentenced to death for his activities, including the bewitching of Redferne's bride-to-be, but before the execution a demon appears and propels the Warlock forward in time to 20th century Los Angeles, California. Redferne follows through the portal.\n" +
                " The Warlock attempts to assemble The Grand Grimoire, a Satanic book that will reveal the \"\"true\"\" name of God.  Redferne and the Warlock then embark on a cat-and-mouse chase with the Grand Grimoire, and Kassandra (Lori Singer), a waitress who encounters Giles while he's attempting to find Warlock.\",11.906872,/pEzCLGxq4bXafTORASwGtLKptLT.jpg,\"[{'name': 'New World Pictures', 'id': 1950}]\",\"[{'iso_3166_1': 'US', 'name': 'United States of America'}]\",1989-06-01,0,103.0,\"[{'iso_639_1': 'en', 'name': 'English'}]\",Released,Satan also has one son.,Warlock,False,5.8,98";
        String str2 = "False,\"{'id': 87536, 'name': 'Warlock Collection', 'poster_path': '/wkwtv5NMpBfo1eGjZpNJEQ2cZH0.jpg', 'backdrop_path': '/im5iJHo8UwswhKHlcADqUO3sZtA.jpg'}\",7000000";
        //Pattern splitter = Pattern.compile("^(True|False),(\"(\\[?\\{(?:(?:'[\\w\\d\\p{Punct}\\s]+':\\s'?['\\w\\d\\p{Punct}\\s]+'?)(?:,\\s)?)+'?}]?)\"|,\"([\\w\\d\\p{Punct}\\s&&[^\\[\\]{}]\n\r\t]+)\"|,([\\d]{1,9}.?[\\d]{0,9})|,([a-zA-Z\\s.]+)|,([\\w\\s]+))++$", Pattern.DOTALL);
        Pattern splitter2 = Pattern.compile("(?=[^\\s]),(?=[^\\s])");
        //Matcher m = splitter.matcher(str);
        String[] split = splitter2.split(str);
        //System.out.println("Split str [" + split.length + "]: " + Arrays.toString(split));
        for (String p : split) {
            LOG.info(p);
        }
    }

    @Test
    public void testDelimiter() throws JSONException, IOException {
        String line = "False,,0,\"[{'id': 80, 'name': 'Crime'}, {'id': 18, 'name': 'Drama'}]\",,74295,tt0086199,fi,Rikos ja rangaistus,\"An adaptation of Dostoyevsky's novel, updated to present-day Helsinki. Slaughterhouse worker Rahikainen murders a man, and is forced to live with the consequences of his actions...\",1.473622,/aqu3HrpHaY8MR2ZOIfuUTWC3r3N.jpg,\"[{'name': 'Villealfa Filmproduction Oy', 'id': 2303}]\",\"[{'iso_3166_1': 'FI', 'name': 'Finland'}]\",1983-12-02,0,93.0,\"[{'iso_639_1': 'fi', 'name': 'suomi'}]\",Released,Crime and Punishment,Crime and Punishment,False,5.9,19";
        try (BufferedReader br = new BufferedReader(new StringReader(line))) {
            String l;
            while ((l = br.readLine()) != null) {
                String delimiter = config.getProperty("csv.regex");
                String[] splitLine = l.split(delimiter);
                System.out.println(splitLine.length + "\n\n");
                JSONObject json = new JSONObject();
                json.put("id", splitLine[5]);
                json.put("budget", splitLine[2]);
                json.put("genre", splitLine[3]);
                json.put("popularity", splitLine[10]);
                json.put("company", splitLine[12]);
                json.put("date", splitLine[14]);
                json.put("revenue", splitLine[15]);
                System.out.println(json.toString());
            }
        }
    }
}