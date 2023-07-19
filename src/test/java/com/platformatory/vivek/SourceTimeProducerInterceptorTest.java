package com.platformatory.vivek;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class SourceTimeProducerInterceptorTest {

    @Test
    public void dummyTest(){
        String testString = "dum" + "my";
        assertEquals("dummy", testString);
    }

    @Test
    public void testOnSend() {
        // Create a dummy ProducerRecord
        String key = "testKey";
        String value = "testValue";
        Headers headers = new RecordHeaders();
        ProducerRecord<String, String> record = new ProducerRecord<>("testTopic", 0, 0L, key, value, headers);

        // Create SourceTimeProducerInterceptor and call onSend
        SourceTimeProducerInterceptor interceptor = new SourceTimeProducerInterceptor();
        ProducerRecord<String, String> newRecord = interceptor.onSend(record);

        // Verify that a new ProducerRecord is created with "RecordLatency" as the topic
        assertEquals("RecordLatency", newRecord.topic());

        // Verify that the UUID header is set and is of valid UUID format
        Headers newHeaders = newRecord.headers();
        assertNotNull(newHeaders.lastHeader("UUID"));
        String uuid = new String(newHeaders.lastHeader("UUID").value(), StandardCharsets.UTF_8);
        assertTrue(uuid.matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"));
    }
}
