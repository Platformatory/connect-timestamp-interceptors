# Kafka Connect Latency Interceptor

Interceptors for measuring the end to end latency in a Kafka connect pipeline. Consists of two interceptors - 

- **SourceTimeProducerInterceptor** - Used to capture the start of the connect pipeline
- **SinkTimeConsumerInterceptor** - Used to capture the end of the connect pipeline and/or other intermediate stages of the pipeline

## Installation

### Manually

- Create a JAR from this repository by running `mvn clean package`
- Copy the JAR file from the target directory to a directory in the `plugin.path` on your connect worker
- Restart the connect worker

## SourceTimeProducerInterceptor

## Working

The SourceTimeProducer Interceptor is a Kafka producer interceptor that captures the source time of a record from the original source and sends it to a configurable telemetry topic for further processing. The interceptor also injects an unique correlation ID (UUID) to the header of the message record for uniquely identifying the record downstream in the connect pipeline.

### Configuration

#### Interceptor classes

```properties
producer.interceptor.classes=com.platformatory.kafka.connect.latencyanalyzer.SourceTimeProducerInterceptor
```

> **These configurations should begin with `producer.` if configured in the worker configuration or begin with `producer.override.` if configured with the connector configuration.**

#### Telemetry topic

The topic to send the telemetry data for further processing

```properties
connect.latency.analyzer.telemetry.topic.name
```

Example - 

```properties
connect.latency.analyzer.telemetry.topic.name=connect_latency_telemetry
```

#### Producer Configuration

The producer configuration for producing the telemetry data to the telemetry topic. Any configuration starting with `latency.interceptor.producer` will be used as the producer configuration.

Example - 

```properties
latency.interceptor.producer.bootstrap.servers=pkc-xxxxx.us-west4.gcp.confluent.cloud:9092
latency.interceptor.producer.security.protocol=SASL_SSL
latency.interceptor.producer.sasl.mechanism=PLAIN
latency.interceptor.producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="xxxxxxx" password="yyyyyyyyyyyyyyyyyy";
```

#### Connect Pipeline ID

Used to uniquely identify a pipeline when the telemetry is sent to the telemetry topic

```properties
connect.pipeline.id
```

Example - 

```properties
connect.pipeline.id=PostgresToMYSQLPipeline
```

#### Source Time Field

The field in the source record value for the timestamp. This value is considered as the start of the connect pipeline.

If the format is not specified using `source.time.format`, it is considered to be in epoch and in java long. If the source record does not contain a time field, you can set the `source.value.format` to a value other than `json` or `avro`. This will use the time when the Interceptor is executed is used as the source time for the connect pipeline.


```properties
source.time.field
```

Example -

```properties
source.time.field=created_at
```

#### Source Time Format

The SimpleDateFormat of the source time field. Optional field. If the source time is not in epoch, the format can be specified using this property.

```properties
source.time.format
```

Example -

```properties
source.time.format="yyyy-MM-dd'T'HH:mm:ss'Z'"
```

#### Source Value Format

The format of the source record for deserializing and extracting the source time field. **Can only be `avro` or `json`**. If the property is set to a value other than `json` or `avro`, the time when the Interceptor is executed is used as the source time for the connect pipeline.

```properties
source.time.format=avro

source.time.format=json
```

#### Source Value Deserialization properties 

If the source time format is avro, these properties are used to deserialize the Avro record. Any configuration starting with `source.value` will be used for deserialization of the source record value.

Example -

```properties
source.value.schema.registry.url=https://psrc-xxxxx.us-central1.gcp.confluent.cloud
source.value.basic.auth.credentials.source=USER_INFO
source.value.schema.registry.basic.auth.user.info=XXXXXXX:YYYYYYYYYYYYYYYYYYYYY
```

#### Sampling Rate

The rate for the sampling the telemetry data. Needs to be a float value between 0 and 1.

```properties
sampling.rate
```

Example -

```properties
sampling.rate=0.5
```

## SinkTimeConsumerInterceptor

## Working

The SinkTime Interceptor is a Kafka croducer interceptor that captures the commit time of a record and sends it to a configurable telemetry topic for further processing. The interceptor reads an unique correlation ID (UUID) from the header of the message record for uniquely identifying the record. The interceptor can be configured for intermediate stages in a connect pipeline such as Streams, etc by configuring the `telemetry.type` to a value other than `sink`. 

### Configuration

#### Interceptor classes

```
consumer.interceptor.classes=com.platformatory.kafka.connect.latencyanalyzer.SinkTimeConsumerInterceptor
```

> **These configurations should begin with `consumer.` if configured in the worker configuration or begin with `consumer.override.` if configured with the connector configuration.**

#### Telemetry topic

The topic to send the telemetry data for further processing

```properties
connect.latency.analyzer.telemetry.topic.name
```

Example - 

```properties
connect.latency.analyzer.telemetry.topic.name=connect_latency_telemetry
```

#### Producer Configuration

The producer configuration for producing the telemetry data to the telemetry topic. Any configuration starting with `latency.interceptor.producer` will be used as the producer configuration.

Example - 

```properties
latency.interceptor.producer.bootstrap.servers=pkc-xxxxx.us-west4.gcp.confluent.cloud:9092
latency.interceptor.producer.security.protocol=SASL_SSL
latency.interceptor.producer.sasl.mechanism=PLAIN
latency.interceptor.producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="xxxxxxx" password="yyyyyyyyyyyyyyyyyy";
```

#### Connect Pipeline ID

Used to uniquely identify a pipeline when the telemetry is sent to the telemetry topic

```properties
connect.pipeline.id
```

Example - 

```properties
connect.pipeline.id=PostgresToMYSQLPipeline
```

#### Telemetry Type

Indicates the stage in the connect pipeline. Should be configured to `sink` if it is the final stage.

```properties
telemetry.type
```

Example -

```properties
telemetry.type=sink
```


