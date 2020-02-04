package org.sympatico.data.client.file;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CsvFileClient {

    private static final Logger LOG  = LoggerFactory.getLogger(CsvFileClient.class);

    private final Pattern splitter;
    private final Map<Integer, String> fields;

    public CsvFileClient(Map<Integer, String> fields, String regex) {
        this.fields = fields;
        splitter = Pattern.compile(regex);
    }

    public long jsonizeFileStream(final InputStream inputStream, final OutputStream outputStream) throws IOException {
        long lineCount = 0L;
        File tempFile = File.createTempFile("test-csv-file", ".tmp");
        tempFile.deleteOnExit();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream((outputStream))))) {
            String line;
            StringBuilder stringBuilder = new StringBuilder();
            while ((line = bufferedReader.readLine()) != null)  {
                try {
                    String splitLine = toJson(line);
                    bufferedWriter.write(splitLine + "\n");
                    lineCount++;
                } catch (ArrayIndexOutOfBoundsException e) {
                    // TODO - add metrics counter
                    System.out.println(line);
                    e.printStackTrace();
                }
            }
        } finally {
            tempFile.delete();
        }
        return lineCount;
    }

    private String toJson(CharSequence line) throws ArrayIndexOutOfBoundsException {
        String[] splitLine = splitter.split(normalize(line));
        JSONObject json = new JSONObject();
        for (Map.Entry<Integer, String> entry : fields.entrySet()) {
            try {
                json.put(entry.getValue(), splitLine[entry.getKey()]);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return json.toString();
    }

    private static String normalize(CharSequence line) {
        return line.toString()
                .replace("\r\n", "\n")
                .replace("\r", "\n");
    }
}
