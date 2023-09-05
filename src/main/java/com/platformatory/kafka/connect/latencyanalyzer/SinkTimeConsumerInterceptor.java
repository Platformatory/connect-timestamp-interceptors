package com.platformatory.kafka.connect.latencyanalyzer;

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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap; 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkTimeConsumerInterceptor implements ConsumerInterceptor<String, byte[]> {

   private static final Logger log = LoggerFactory.getLogger(SourceTimeProducerInterceptor.class);

    public KafkaProducer<String, GenericRecord>  producer;
    private String latencyTopic;
    private String sourceTimeField;
    private String connectPipelineID;
    private String telemetryType;
    private Schema schema;

    static String topicConfig             = "connect.latency.analyzer.telemetry.topic.name";
    static String connectPipelineIDConfig = "connect.pipeline.id";
    static String telemetryTypeConfig     = "telemetry.type";
    private final ConcurrentHashMap<Long, String> offsetToUUID = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        String schemaString = "{\"type\":\"record\"," +
                    "\"name\":\"com.platformatory.kafka.connect.latencyanalyzer.Timestamps\"," +
                    "\"fields\":[{\"name\": \"correlation_id\", \"type\": \"string\"}," +
        "{\"name\": \"connect_pipeline_id\", \"type\": \"string\"}," +
        "{\"name\": \"timestamp_type\", \"type\": \"string\"}," +
        "{\"name\": \"timestamp\", \"type\": \"long\"}]}";
        schema = new Schema.Parser().parse(schemaString);

        Map<String, String> producerProps = new HashMap<>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            String key = entry.getKey();
            
            if (key.startsWith("latency.interceptor.producer.")) {
                producerProps.put(key.replace("latency.interceptor.producer.", ""), entry.getValue().toString());
            }
        }

        Properties props = new Properties();
        props.putAll(producerProps);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);

        log.debug("Latency Consumer Interceptor Producer configurations - "+props.toString());

        producer = new KafkaProducer<String, GenericRecord>(props);

        latencyTopic = (String) configs.get(topicConfig);
        connectPipelineID = (String) configs.get(connectPipelineIDConfig);
        telemetryType = (String) configs.get(telemetryTypeConfig);

    }

    @Override
    public ConsumerRecords<String, byte[]> onConsume(ConsumerRecords<String, byte[]> records) {

        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);

            for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                String uuid = null;
                for (Header header : record.headers()) {
                    if (header.key().equals("connect_latency_correlation_id")) {
                        uuid = new String(header.value(), StandardCharsets.UTF_8);
                        break;
                    }
                }

                if (uuid != null) {
                    offsetToUUID.put(record.offset(), uuid);
                }
            }
        }

        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        Long commitTime = System.currentTimeMillis();

        for(Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
            Long offset = entry.getValue().offset();
            String correlation_id = offsetToUUID.get(offset);

            if (correlation_id != null) {
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("correlation_id", correlation_id);
                avroRecord.put("connect_pipeline_id", connectPipelineID);
                avroRecord.put("timestamp_type", telemetryType);
                avroRecord.put("timestamp", commitTime);

                try {
                    producer.send(new ProducerRecord<String, GenericRecord> (latencyTopic, connectPipelineID, avroRecord));
                    offsetToUUID.remove(offset);
                } catch(SerializationException e) {
                    // may need to do something with it
                    // TODO: Handle exception
                    e.printStackTrace();
                }
                finally {
                    producer.flush();
                }
            }
        }

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
