package com.barley.orleans.broker;

import com.barley.orleans.exceptions.InvalidPayloadException;
import com.barley.orleans.interfaces.AfterCall;
import com.barley.orleans.structure.Payload;
import com.barley.orleans.structure.Response;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Producer class to publish/produce payloads to Kafka broker.
 */
public final class Producer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static KafkaProducer<String, String> kafkaProducer = null;
    private static MockProducer<String, String> mockProducer = null;
    private static Producer producer = null;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static boolean isProducerAlive = false;
    private static Properties producerProperties = null;

    private Producer(Properties properties, boolean isMock) {
        producerProperties = properties;
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        if (!isMock)
            kafkaProducer = new KafkaProducer<>(properties);
        isProducerAlive = true;
    }

    public static Producer producer(Properties properties) {
        return producer(properties, false);
    }

    public static Producer producer(Properties properties, boolean isMock) {
        if (!isProducerAlive || producer == null || kafkaProducer == null) {
            producer = new Producer(properties, isMock);
        }
        return producer;
    }

    public Response produce(String topic, Payload payload) {
        return produce(topic, null, null, payload, null);
    }

    public Response produce(String topic, Payload payload, AfterCall afterCall) {
        return produce(topic, null, null, payload, afterCall);
    }

    public Response produce(String topic, Integer partition, Payload payload) {
        return produce(topic, partition, null, payload, null);
    }

    public Response produce(String topic, String key, Payload payload) {
        return produce(topic, null, key, payload, null);
    }

    public Response produce(String topic, Integer partition, Payload payload, AfterCall afterCall) {
        return produce(topic, partition, null, payload, afterCall);
    }

    public Response produce(String topic, String key, Payload payload, AfterCall afterCall) {
        return produce(topic, null, key, payload, afterCall);
    }

    public Response produce(String topic, Integer partition, String key, Payload payload, AfterCall afterCall) {
        final Response response = new Response();
        try {
            if (topic == null || topic.length() == 0)
                throw new InvalidPayloadException("Topic cannot be null or empty");
            validateInput(payload);
            //final Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<>(topic, partition, key, OBJECT_MAPPER.writeValueAsString(payload)), (metadata, exception) -> afterCall.after(topic, metadata.partition(), metadata.offset(), exception, payload, producerProperties));
            final Future<RecordMetadata> future = mockProducer.send(new ProducerRecord<>(topic, partition, key, OBJECT_MAPPER.writeValueAsString(payload)), (metadata, exception) -> {
                if (afterCall != null)
                    afterCall.after(topic, metadata.partition(), metadata.offset(), exception, payload, producerProperties);
            });
            RecordMetadata recordMetadata = future.get(100, TimeUnit.MILLISECONDS);
            response.setOffset(recordMetadata.offset());
            response.setPartition(recordMetadata.partition());
        } catch (IOException | InterruptedException | ExecutionException | TimeoutException | InvalidPayloadException e) {
            logger.error("Error while producing record to kafka for topic: " + topic + " with payload: " + payload, e);
            response.addError(e.getLocalizedMessage());
        }
        return response;
    }

    private void validateInput(Payload payload) throws InvalidPayloadException {
        String exception = null;
        if (payload == null)
            exception = "Payload cannot be null";
        else if (payload.getData() == null)
            exception = "Payload data cannot be null, please provide valid data";
        else if (payload.getClient() == null || payload.getClient().length() == 0)
            exception = "Client value cannot be null or empty";
        else if (payload.getIpAddress() == null || payload.getIpAddress().length() == 0)
            exception = "Provide a valid IP Address";

        if (exception != null)
            throw new InvalidPayloadException(exception);
    }


    public List<PartitionInfo> partitionInfo(String topic) throws IOException {
        List<PartitionInfo> partitionInfoList = null;
        if (kafkaProducer != null && producer != null)
            partitionInfoList = kafkaProducer.partitionsFor(topic);
        else if (mockProducer != null) {
            partitionInfoList = mockProducer.partitionsFor(topic);
        } else
            throwIOException("Kafka Producer is not initialized or is closed. Please initialize the producer before invoking this method");
        return partitionInfoList;
    }

    public String metrics() throws IOException {
        String metrics = null;
        if (kafkaProducer != null && producer != null)
            metrics = OBJECT_MAPPER.writeValueAsString(kafkaProducer.metrics());
        else if (mockProducer != null) {
            metrics = OBJECT_MAPPER.writeValueAsString(mockProducer.metrics());
        } else
            throwIOException("Kafka Producer is not initialized or is closed. Please initialize the producer before invoking this method");
        return metrics;
    }

    private void throwIOException(String exception) throws IOException {
        throw new IOException(exception);
    }
}
