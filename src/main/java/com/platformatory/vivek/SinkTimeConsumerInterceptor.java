package com.platformatory.vivek;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SinkTimeConsumerInterceptor implements ConsumerInterceptor<String, String> {

    public KafkaProducer<String, String> producer;

    @Override
    public void configure(Map<String, ?> map) {
        // This method allows you to retrieve the configuration of the interceptor from the configuration properties
        // Initialize Kafka Producer with necessary configurations for the RecordLatency topic
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {

        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();

        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

            for (ConsumerRecord<String, String> record : partitionRecords) {
                // Get the UUID from the headers
                String uuid = null;
                for (Header header : record.headers()) {
                    if (header.key().equals("UUID")) {
                        uuid = new String(header.value(), StandardCharsets.UTF_8);
                        break;
                    }
                }

                // If UUID is not null, send the current time to the RecordLatency topic with the UUID as the key
                if (uuid != null) {
                    String currentTimeMillis = String.valueOf(System.currentTimeMillis());
                    producer.send(new ProducerRecord<>("RecordLatency", uuid, currentTimeMillis));
                }
            }

            // Add newPartitionRecords to recordMap
            recordMap.put(partition, partitionRecords);
        }

        // Return the original records without modifying them
        return new ConsumerRecords<>(recordMap);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        // This method is called after the offsets are committed. The SinkTime is set when the record is consumed, so nothing is required here.
    }

    @Override
    public void close() {
        // This method is called when the interceptor is being shut down
        producer.close();
    }
}

