package org.sympatico.data.client.file;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

public class CsvFile {

    private static final Logger LOG  = LoggerFactory.getLogger(CsvFile.class);

    public static void jsonize(String filename,
                               Map<String, Integer> fields,
                               Boolean hasHeader,
                               String key,
                               String regex,
                               ConcurrentLinkedQueue<Pair<String, byte[]>> queue) throws IOException {
        LOG.info("Reading file: " + filename);
        File inputFile = new File(filename);
        File tempFile = File.createTempFile("csvfile", ".tmp");
        //tempFile.deleteOnExit();
        System.out.println("Temp file: " + tempFile.getAbsolutePath());
        normalize(inputFile, tempFile);
        Pattern splitter = Pattern.compile(regex);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(tempFile)))) {
            String line;
            if (hasHeader)
                br.readLine(); // remove header
            while ((line = br.readLine()) != null) {
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
                queue.add(new ImmutablePair<>(key, json.toString().getBytes(StandardCharsets.UTF_8)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info("File loading completed for " + key );
    }

    private static void normalize(File inputFile, File tempFile) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile)));
        int count = 0;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                try {
                    line.replace("\"", "").replace("\r\n", "\n").replace("\r", "\n");
                    bufferedWriter.write(line + "\n");
                } catch (Exception e) {
                    System.out.println("Unable to normalize line: " + line);
                    e.printStackTrace();
                }
            }
            bufferedWriter.flush();
        }
        System.out.println("Line count: " + count);
    }

}
