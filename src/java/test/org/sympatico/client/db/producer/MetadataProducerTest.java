package org.sympatico.client.db.producer;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MetadataProducerTest {

    private static final ConcurrentLinkedQueue<Pair<String, byte[]>> queue = new ConcurrentLinkedQueue<>();

    private final Properties config = new Properties();
    @Before
    public void setup() throws Exception {
        config.load(new FileInputStream(new File("conf/client.test.properties")));
    }

    @Test
    public void upload() throws Exception {

    }
}