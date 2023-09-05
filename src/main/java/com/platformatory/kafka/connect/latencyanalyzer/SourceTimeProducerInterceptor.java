package com.platformatory.kafka.connect.latencyanalyzer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceTimeProducerInterceptor implements ProducerInterceptor<String, byte[]> {

    private static final Logger log = LoggerFactory.getLogger(SourceTimeProducerInterceptor.class);

    public KafkaProducer<String, GenericRecord>  producer;
    public KafkaAvroDeserializer deserializer;
    private String latencyTopic;
    private String sourceTimeField;
    private String connectPipelineID;
    private String sourceRecordValueFormat;
    private float samplingRate;
    private Schema schema;

    static String topicConfig                   = "connect.latency.analyzer.telemetry.topic.name";
    static String sourceTimeFieldConfig         = "source.time.field";
    static String connectPipelineIDConfig       = "connect.pipeline.id";
    static String sourceRecordValueFormatConfig = "source.value.format"; // Can only be 'json' or 'avro'
    static String samplingConfig                = "sampling.rate";
    // TODO: Sampling

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO: Create topic if not exists?

        // TODO: Read schema from file
        String schemaString = "{\"type\":\"record\"," +
                    "\"name\":\"com.platformatory.kafka.connect.latencyanalyzer.Timestamps\"," +
                    "\"fields\":[{\"name\": \"correlation_id\", \"type\": \"string\"}," +
        "{\"name\": \"connect_pipeline_id\", \"type\": \"string\"}," +
        "{\"name\": \"timestamp_type\", \"type\": \"string\"}," +
        "{\"name\": \"timestamp\", \"type\": \"long\"}]}";
        schema = new Schema.Parser().parse(schemaString);

        Map<String, String> producerProps = new HashMap<>();
        Map<String, String> schemaRegistryProps = new HashMap<>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            String key = entry.getKey();
            
            if (key.startsWith("latency.interceptor.producer.")) {
                producerProps.put(key.replace("latency.interceptor.producer.", ""), entry.getValue().toString());
            } else if (key.startsWith("source.value.")) {
                schemaRegistryProps.put(key.replace("source.value.", ""), entry.getValue().toString());
            }
        }

        Properties props = new Properties();
        props.putAll(producerProps);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);

        log.info("Producer configurations - "+producerProps.toString());

        producer = new KafkaProducer<String, GenericRecord>(props);
        

        latencyTopic = (String) configs.get(topicConfig);
        sourceTimeField = (String) configs.get(sourceTimeFieldConfig);
        connectPipelineID = (String) configs.get(connectPipelineIDConfig);
        sourceRecordValueFormat = (String) configs.get(sourceRecordValueFormatConfig);
        samplingRate = Float.parseFloat((String) configs.get(samplingConfig));
        if (sourceRecordValueFormat.equalsIgnoreCase("avro")) {
            deserializer = new KafkaAvroDeserializer();
            deserializer.configure(schemaRegistryProps, false);
        }
    }

    @Override
    public ProducerRecord<String, byte[]> onSend(ProducerRecord<String, byte[]> record) {
        String uuid = UUID.randomUUID().toString();

        record.headers().add("connect_latency_correlation_id", uuid.getBytes());
        

        // TODO: Convert the source timestamp value from string/long
        // TODO: JSON Path
        Long sourceTimestamp = System.currentTimeMillis();
        if (sourceRecordValueFormat.equalsIgnoreCase("json")) {
            String valueStr = new String(record.value());
            ObjectMapper objectMapper = new ObjectMapper();
            
            try {
                JsonNode jsonNode = objectMapper.readTree(valueStr);
                sourceTimestamp = jsonNode.get("payload").get(sourceTimeField).longValue();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (sourceRecordValueFormat.equalsIgnoreCase("avro")) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                final Object o = deserializer.deserialize(record.topic(), record.value());

                GenericRecord sourceRecord = (GenericRecord) o;

                JsonNode jsonNode = objectMapper.readTree(sourceRecord.toString());
                sourceTimestamp = jsonNode.get(sourceTimeField).longValue();
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("correlation_id", uuid);
        avroRecord.put("connect_pipeline_id", connectPipelineID);
        avroRecord.put("timestamp_type", "source");
        avroRecord.put("timestamp", sourceTimestamp);

        try {
            producer.send(new ProducerRecord<String, GenericRecord> (latencyTopic, connectPipelineID, avroRecord));
        } catch(SerializationException e) {
            // may need to do something with it
            // TODO: Handle exception
            e.printStackTrace();
        }
        finally {
            producer.flush();
        }

        return record;
    }


    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // This method is called when the broker acknowledges the receipt of the record
    }

    @Override
    public void close() {
        // This method is called when the interceptor is being shut down
        if(producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
