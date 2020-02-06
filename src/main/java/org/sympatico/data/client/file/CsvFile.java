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

    public static long jsonize(String inputPath,
                               Map<String, Integer> fields,
                               String regex,
                               String outputPath) throws IOException {
        LOG.info("Jsonizing file: " + inputPath);
        long lineCount = 0L;
        Pattern splitter = Pattern.compile(regex);
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath)));
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

    public static long writeNormalizedFile(String inputPath, String outputPath)
            throws IOException {
        LOG.info("Normalizing file: " + inputPath);
        long lineCount = 0L;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath)));
             BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath)))) {
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
