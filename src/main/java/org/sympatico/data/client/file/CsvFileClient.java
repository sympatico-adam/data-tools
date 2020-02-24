package org.sympatico.data.client.file;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.CharBuffer;
import java.util.Map;
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
             BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(tempFile))))) {
            String line;
            while ((line = bufferedReader.readLine()) != null)  {
                bufferedWriter.write(normalize(line) + "\n");
            }
            bufferedWriter.flush();
        }
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(tempFile))));
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream((outputStream))))) {
            String line;
            while ((line = bufferedReader.readLine()) != null)  {
                final StringBuilder stringBuilder = new StringBuilder();
                int chunkSize = 100;
                try {
                    for (int i = 0; i < chunkSize; i++) {
                        if (line.matches("^[\\s].*")) {
                            final StringBuffer buffer = new StringBuffer();
                            lineCount += combineLines(bufferedReader, buffer);
                        } else {
                            lineCount++;
                            bufferedWriter.write(splitLine(line) + "\n");
                        }
                    }
                } catch (JSONException|ArrayIndexOutOfBoundsException e) {
                    // TODO - add metrics counter
                    System.out.println(line);
                    e.printStackTrace();
                }
                bufferedWriter.flush();
            }
        } finally {
            tempFile.delete();
        }
        return lineCount;
    }

    private String splitLine(CharSequence line) throws JSONException, ArrayIndexOutOfBoundsException {
        String[] split = splitter.split(normalize(line));
        JSONObject json = new JSONObject();
        for (Map.Entry<Integer, String> entry : fields.entrySet()) {
            json.put(entry.getValue(), split[entry.getKey()]);
        }
        return json.toString();
    }

    private static int combineLines(final BufferedReader bufferedReader, final StringBuffer buffer)
            throws IOException {
        String line;
        int lineCount = 0;
        while (((line = bufferedReader.readLine()) != null)) {
            buffer.append(line).append("\n");
            lineCount++;
            if (!line.matches("^[\\s]+.*")) {
                break;
            }
        }
        return lineCount;
    }

    private static String normalize(CharSequence line) {
        return line.toString()
                .replace("\n\t", "\t")
                .replace("\r\n", "\n")
                .replace("\r", "\n");
    }
}
