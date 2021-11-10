package org.sympatico.data.client.file

import org.codehaus.jettison.json.JSONException
import org.codehaus.jettison.json.JSONObject
import org.slf4j.LoggerFactory
import org.sympatico.data.client.file.CsvFileClient
import java.io.*

class CsvFileClient(private val fields: Map<Int, String>, regex: String) {
    private val splitter: Regex = Regex(regex)

    @Throws(IOException::class)
    fun jsonizeFileStream(inputStream: InputStream, outputStream: OutputStream): Long {
        var lineCount = 0L
        val tempFile = File.createTempFile("test-csv-file", ".tmp")
        tempFile.deleteOnExit()
        try {
            BufferedReader(InputStreamReader(BufferedInputStream(inputStream))).use { bufferedReader ->
                BufferedWriter(
                    OutputStreamWriter(
                        BufferedOutputStream(
                            outputStream
                        )
                    )
                ).use { bufferedWriter ->
                    var line: String
                    val stringBuilder = StringBuilder()
                    while (bufferedReader.readLine().also { line = it } != null) {
                        try {
                            val splitLine = toJson(line)
                            bufferedWriter.write(
                                """
    $splitLine
    
""".trimIndent()
                            )
                            lineCount++
                        } catch (e: ArrayIndexOutOfBoundsException) {
                            // TODO - add metrics counter
                            println(line)
                            e.printStackTrace()
                        }
                    }
                }
            }
        } finally {
            tempFile.delete()
        }
        return lineCount
    }

    @Throws(ArrayIndexOutOfBoundsException::class)
    private fun toJson(line: CharSequence): String {
        val splitLine = splitter.split(normalize(line))
        val json = JSONObject()
        for ((key, value) in fields) {
            try {
                json.put(value, splitLine[key])
            } catch (e: JSONException) {
                e.printStackTrace()
            }
        }
        return json.toString()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(CsvFileClient::class.java)
        private fun normalize(line: CharSequence): String {
            return line.toString()
                .replace("\r\n", "\n")
                .replace("\r", "\n")
        }
    }

}