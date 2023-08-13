package com.platformatory.vivek;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class SourceTimeProducerInterceptor implements ProducerInterceptor<String, byte[]> {

    public KafkaProducer<String, String> producer;
    private String latencyTopic;
    private String sourceTimeField;
    private String ConnectPipelineID;
    private String TelemetryType;

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
        sourceTimeField = (String) configs.get("source.time.field");
        ConnectPipelineID = (String) configs.get("connect.pipeline.id");
        TelemetryType = (String) configs.get("telemetry.Type");
    }

    @Override
    public ProducerRecord<String, byte[]> onSend(ProducerRecord<String, byte[]> record) {
        // Generate a completely unique UUID for each record
        String uuid = UUID.randomUUID().toString();

        // Add the UUID to existing headers
        record.headers().add("UUID", uuid.getBytes());

        String valueStr = new String(record.value());


        // Create JSON to get createdOn value
        ObjectMapper objectMapper = new ObjectMapper();
        String createdOn = "NA";
        try {
            JsonNode jsonNode = objectMapper.readTree(valueStr);
            createdOn = jsonNode.get("payload").get(sourceTimeField).asText();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Create JSON string to send
        Map<String, String> jsonMap = new HashMap<>();
        jsonMap.put("Correlation ID", uuid);
        jsonMap.put("Connect Pipeline ID", ConnectPipelineID);
        jsonMap.put("Telemetry Type", TelemetryType);
        jsonMap.put("SourceTime", createdOn);
        jsonMap.put("SinkTime", "");

        String jsonToSend = "";
        try {
            jsonToSend = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        // Send the record to the RecordLatency topic
        producer.send(new ProducerRecord<>(latencyTopic, ConnectPipelineID, jsonToSend));

        // Create a new ProducerRecord that includes the new headers and return it
        return record;
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
