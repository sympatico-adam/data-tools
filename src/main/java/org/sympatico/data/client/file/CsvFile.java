package org.sympatico.data.client.file;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
        File file = new File(filename);
        Pattern splitter = Pattern.compile(regex);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            String line;
            if (hasHeader)
                br.readLine(); // remove header
            while ((line = br.readLine()) != null) {
                line.replace("\"", "").replace("\n", "\r").replace("\t", "\f");
            }
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            String line;
            if (hasHeader)
                br.readLine(); // remove header
            while ((line = br.readLine()) != null) {
                String[] split = splitter.split(line);
                JSONObject json = new JSONObject();
                for (Map.Entry<String, Integer> entry : fields.entrySet()) {
                    try {
                        json.put(entry.getKey(), split[entry.getValue()]);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        e.printStackTrace();
                    }
                }
                queue.add(new ImmutablePair<>(key, json.toString().getBytes(StandardCharsets.UTF_8)));
            }
        } catch (JSONException | IOException e) {
            LOG.error("Unable to format json: " + e);
            e.printStackTrace();
        }
        LOG.info("File loading completed for " + key );
    }

}
