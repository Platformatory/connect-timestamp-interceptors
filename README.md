# Kafka Producer and Consumer Interceptors

This project demonstrates the use of Apache Kafka's Producer and Consumer Interceptors for preprocessing records as they're produced and consumed.

## Project Structure

The project is comprised of two main classes:

1. `SourceTimeProducerInterceptor`: This is a Producer Interceptor which creates a UUID of each produced record. It sends a new record with the UUID as 'Key' and the current timestamp as 'Value' to the "RecordLatency" topic. And adds the UUID to each records headers.

2. `SinkTimeConsumerInterceptor`: This is a Consumer Interceptor that extracts a UUID from the headers of each consumed record and sends a new record with the UUID and the current timestamp to the "RecordLatency" topic.

## Dependencies

This project requires:

- Java 8 or above
- Apache Kafka
- Kafka's Java client library

## Running the Code

Firstly, you need to set up a Kafka cluster. Confluent's quick start guide can be used for this: [Confluent Quickstart](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html)

Next, you need to configure your producer and consumer to use the interceptors. For the producer:

```java
props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.platformatory.kafka.SourceTimeProducerInterceptor");

```

For the Consumer:

```java
props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.platformatory.kafka.SinkTimeConsumerInterceptor");

```
You can then run your producer and consumer as normal, and they will use the interceptors.

## Testing
Unit tests for the interceptors can be found in the src/test/java directory. They can be run using your preferred testing framework, such as JUnit.
