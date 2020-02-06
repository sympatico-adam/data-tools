package org.sympatico.data.client.file;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.regex.Pattern;

public class CsvFile {

    private static final Logger LOG  = LoggerFactory.getLogger(CsvFile.class);

    private static final String TEMP_FILE_NAME = "csv-temp-file";
    private static final String TEMP_FILE_SUFFIX = ".tmp";

    public static long jsonize(String inputFilename,
                               Map<String, Integer> fields,
                               String regex,
                               String outputPath) throws IOException {
        LOG.info("Jsonizing file: " + inputFilename);
        long lineCount = 0L;
        Pattern splitter = Pattern.compile(regex);
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilename)));
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath)))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lineCount++;
                String[] split = splitter.split(line);
                JSONObject json = new JSONObject();
                for (Map.Entry<String, Integer> entry : fields.entrySet()) {
                    try {
                        json.put(entry.getKey(), split[entry.getValue()]);
                    } catch (ArrayIndexOutOfBoundsException | JSONException e) {
                        e.printStackTrace();
                        LOG.error("Failed to parse line: " + entry.getValue());
                    }
                }
                bufferedWriter.write(json.toString() + "\n");
            }
            bufferedWriter.flush();
        }
        return lineCount;
    }

    public static long writeNormalizedFile(String inputFilePath, String outputFilePath)
            throws IOException {
        LOG.info("Normalizing file: " + inputFilePath);
        long lineCount = 0L;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath)));
             BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFilePath)))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lineCount++;
                bufferedWriter.write(
                        line.replace("\"", "")
                                .replace("\r\n", "\n")
                                .replace("\r", "\n") + "\n");
            }
            bufferedWriter.flush();
        }
        return lineCount;
    }

}
