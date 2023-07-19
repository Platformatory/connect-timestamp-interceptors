package com.platformatory.vivek;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        // Set the properties for the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // source
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.platformatory.vivek.SourceTimeProducerInterceptor");

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Use a try-catch to ensure the producer is closed properly
        try {
            for (int i = 0; i < 50; i++) {
                // Create a new record that we want to send
                ProducerRecord<String, String> record = new ProducerRecord<>("test", "vivek", Integer.toString(i));
                // Send the record to the producer
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            // If there is an exception print it
                            exception.printStackTrace();
                        } else {
                            System.out.printf("Sent record(key=%s value=%s) " +
                                            "meta(partition=%d, offset=%d)\n",
                                    record.key(), record.value(), metadata.partition(),
                                    metadata.offset());
                        }
                    }
                });

                // Sleep for 100 ms
                Thread.sleep(9000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Ensure the producer is closed
            producer.close();
        }
    }
}
