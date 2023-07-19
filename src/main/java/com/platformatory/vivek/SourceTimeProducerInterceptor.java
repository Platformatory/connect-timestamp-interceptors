package com.platformatory.vivek;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class SourceTimeProducerInterceptor implements ProducerInterceptor<String, String> {

    public KafkaProducer<String, String> producer;

    @Override
    public void configure(Map<String, ?> configs) {
        // This method allows you to retrieve the configuration of the interceptor from the configuration properties
        // Initialize Kafka Producer with necessary configurations for the RecordLatency topic
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // Generate a completely unique UUID for each record
        String uuid = UUID.randomUUID().toString();

        // Create new headers and add the UUID to it
        Headers newHeaders = new RecordHeaders(record.headers());
        newHeaders.add("UUID", uuid.getBytes(StandardCharsets.UTF_8));

        String sourceTimeAsValue = String.valueOf(System.currentTimeMillis());

        // Send the record to the RecordLatency topic
        producer.send(new ProducerRecord<>("RecordLatency", uuid, sourceTimeAsValue));

        // Create a new ProducerRecord that includes the new headers
        return new ProducerRecord<>(
                record.topic(),
                record.partition(),
                record.timestamp(),
                record.key(),
                record.value(),
                newHeaders
        );
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // This method is called when the broker acknowledges the receipt of the record
    }

    @Override
    public void close() {
        // This method is called when the interceptor is being shut down
        producer.close();
    }
}
