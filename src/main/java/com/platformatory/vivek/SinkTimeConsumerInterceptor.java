package com.platformatory.vivek;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SinkTimeConsumerInterceptor implements ConsumerInterceptor<String, byte[]> {

    public KafkaProducer<String, String> producer;
    private String latencyTopic;
    private String ConnectPipelineID;
    private String TelemetryType;
    private final Map<Long, String> offsetToUUID = new HashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        // This method allows you to retrieve the configuration of the interceptor from the configuration properties
        // Initialize Kafka Producer with necessary configurations for the RecordLatency topic
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);

        latencyTopic = (String) configs.get("connect.latency.analyzer.telemetry.topic.name");
        ConnectPipelineID = (String) configs.get("connect.pipeline.id");
        TelemetryType = (String) configs.get("telemetry.Type");

    }

    @Override
    public ConsumerRecords<String, byte[]> onConsume(ConsumerRecords<String, byte[]> records) {

        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> recordMap = new HashMap<>();

        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);

            for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                // Get the UUID from the headers
                String uuid = null;
                for (Header header : record.headers()) {
                    if (header.key().equals("UUID")) {
                        uuid = new String(header.value(), StandardCharsets.UTF_8);
                        break;
                    }
                }

                // If UUID is not null, store it in the map with its offset
                if (uuid != null) {
                    offsetToUUID.put(record.offset(), uuid);
                }

                // Add newPartitionRecords to recordMap
                recordMap.put(partition, partitionRecords);
            }
        }

        // Return the original records without modifying them
        return new ConsumerRecords<>(recordMap);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        // This method is called after the offsets are committed.
        String commitTime = String.valueOf(System.currentTimeMillis());

        // Iterate over the offsetToUUID map instead of the incoming map
        for (Map.Entry<Long, String> entry : offsetToUUID.entrySet()) {
            Long offset = entry.getKey();
            String uuid = entry.getValue();

            // Prepare the json
            Map<String, String> jsonMap = new HashMap<>();
            jsonMap.put("Correlation ID", uuid);
            jsonMap.put("SinkTime", commitTime);
            jsonMap.put("Connect Pipeline ID", ConnectPipelineID);
            jsonMap.put("Telemetry Type", TelemetryType);
            jsonMap.put("SourceTime", "");

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonToSend = "";
            try {
                jsonToSend = objectMapper.writeValueAsString(jsonMap);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            // Send the record to the RecordLatency topic
            producer.send(new ProducerRecord<>(latencyTopic, ConnectPipelineID, jsonToSend));
        }

        // Clear the offsetToUUID map to save memory
        offsetToUUID.clear();

    }

    @Override
    public void close() {
        // This method is called when the interceptor is being shut down
        producer.close();
    }
}
