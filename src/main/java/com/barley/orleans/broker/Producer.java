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
    private static boolean isMockProducer = false;
    private static Properties producerProperties = null;

    /**
     * Producer private constructor for internal use
     *
     * @param properties Properties needed for Producer
     * @param isMock     is this producer a mock one (Only for testing purposes, should be always false in production environment)
     */
    private Producer(Properties properties, boolean isMock) {
        producerProperties = properties;
        if (!isMock)
            kafkaProducer = new KafkaProducer<>(properties);
        else {
            mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
            isMockProducer = true;
        }
        isProducerAlive = true;
    }

    /**
     * static access method to retrieve the alive Producer instance. This is the main method to access the Producer instance.
     *
     * @param properties Properties needed for Producer
     * @return Producer already alive or newly created singleton instance of Producer
     */
    public static Producer producer(Properties properties) {
        return producer(properties, false);
    }

    /**
     * static access method to retrieve the alive Producer instance. Added to allow creation of a mock producer for testing purposes.
     *
     * @param properties Properties needed for Producer
     * @param isMock     is this producer a mock one (Only for testing purposes, should be always false in production environment)
     * @return already alive or newly created singleton instance of Producer
     */
    public static Producer producer(Properties properties, boolean isMock) {
        if (!isProducerAlive || producer == null || kafkaProducer == null) {
            producer = new Producer(properties, isMock);
        }
        return producer;
    }

    /**
     * Method to produce a payload object to provided topic in kafka.
     * If topic doesn't exist in system, it will be created if <i>auto.create.topics.enable</i> is true for kafka broker.
     *
     * @param topic   the topic to which data need to produced.
     * @param payload Payload which need to be produced.
     * @return Response containing offset, partition info if produce was successful else list of error strings.
     */
    public Response produce(String topic, Payload payload) {
        return produce(topic, null, null, payload, null);
    }

    /**
     * Method to produce a payload object to provided topic in kafka. Client can provide a AfterCall implementation to be executed.
     * If topic doesn't exist in system, it will be created if <i>auto.create.topics.enable</i> is true for kafka broker.
     *
     * @param topic     the topic to which data need to produced.
     * @param payload   Payload which need to be produced.
     * @param afterCall AfterCall interface which user can implement to allow API to invoke after the call of a method is complete.
     * @return Response containing offset, partition info if produce was successful else list of error strings.
     */
    public Response produce(String topic, Payload payload, AfterCall afterCall) {
        return produce(topic, null, null, payload, afterCall);
    }

    /**
     * Method to produce a payload object to provided topic and partition in kafka.
     * If topic doesn't exist in system, it will be created if <i>auto.create.topics.enable</i> is true for kafka broker.
     *
     * @param topic     the topic to which data need to produced.
     * @param partition Partition in kafka where payload need to be produced.
     * @param payload   Payload which need to be produced.
     * @return Response containing offset, partition info if produce was successful else list of error strings.
     */
    public Response produce(String topic, Integer partition, Payload payload) {
        return produce(topic, partition, null, payload, null);
    }

    /**
     * Method to produce a payload object to provided topic and key for partitioning.
     * If topic doesn't exist in system, it will be created if <i>auto.create.topics.enable</i> is true for kafka broker.
     *
     * @param topic   the topic to which data need to produced.
     * @param key     the key to be used for partitioning.
     * @param payload Payload which need to be produced.
     * @return Response containing offset, partition info if produce was successful else list of error strings.
     */
    public Response produce(String topic, String key, Payload payload) {
        return produce(topic, null, key, payload, null);
    }

    /**
     * Method to produce a payload object to provided topic in kafka and partition in kafka. Client can provide a AfterCall implementation to be executed.
     * If topic doesn't exist in system, it will be created if <i>auto.create.topics.enable</i> is true for kafka broker.
     *
     * @param topic     the topic to which data need to produced.
     * @param partition Partition in kafka where payload need to be produced.
     * @param payload   Payload which need to be produced.
     * @param afterCall AfterCall interface which user can implement to allow API to invoke after the call of a method is complete.
     * @return Response containing offset, partition info if produce was successful else list of error strings.
     */
    public Response produce(String topic, Integer partition, Payload payload, AfterCall afterCall) {
        return produce(topic, partition, null, payload, afterCall);
    }

    /**
     * Method to produce a payload object to provided topic in kafka and key for partitioning. Client can provide a AfterCall implementation to be executed.
     * If topic doesn't exist in system, it will be created if <i>auto.create.topics.enable</i> is true for kafka broker.
     *
     * @param topic     the topic to which data need to produced.
     * @param key       the key to be used for partitioning.
     * @param payload   Payload which need to be produced.
     * @param afterCall AfterCall interface which user can implement to allow API to invoke after the call of a method is complete.
     * @return Response containing offset, partition info if produce was successful else list of error strings.
     */
    public Response produce(String topic, String key, Payload payload, AfterCall afterCall) {
        return produce(topic, null, key, payload, afterCall);
    }

    /**
     * private method to support other produce methods.
     *
     * @param topic     the topic to which data need to produced.
     * @param partition Partition in kafka where payload need to be produced.
     * @param key       the key to be used for partitioning.
     * @param payload   Payload which need to be produced.
     * @param afterCall AfterCall interface which user can implement to allow API to invoke after the call of a method is complete.
     * @return Response containing offset, partition info if produce was successful else list of error strings.
     */
    private Response produce(String topic, Integer partition, String key, Payload payload, AfterCall afterCall) {
        final Response response = new Response();
        try {
            if (topic == null || topic.length() == 0)
                throw new InvalidPayloadException("Topic cannot be null or empty");
            validateInput(payload);
            final Future<RecordMetadata> future;
            if (isMockProducer)
                future = send(mockProducer, topic, partition, key, payload, afterCall);

            else
                future = send(kafkaProducer, topic, partition, key, payload, afterCall);
            RecordMetadata recordMetadata = future.get(100, TimeUnit.MILLISECONDS);
            response.setOffset(recordMetadata.offset());
            response.setPartition(recordMetadata.partition());
        } catch (IOException | InterruptedException | ExecutionException | TimeoutException | InvalidPayloadException e) {
            logger.error("Error while producing record to kafka for topic: " + topic + " with payload: " + payload, e);
            response.addError(e.getLocalizedMessage());
        }
        return response;
    }

    /**
     * private method to support producing. It uses MockProducer if enabled else to KafkaProducer.
     *
     * @param producerInterface The producer to be used.
     * @param topic             the topic to which Payload need to be produced.
     * @param partition         Partition to which Payload will be produced to.
     * @param key               the key used for partitioning.
     * @param payload           the payload to be produced.
     * @param afterCall         AfterCall interface which user can implement to allow API to invoke after the call of a method is complete.
     * @return Future with RecordMetadata
     * @throws IOException
     */
    private Future<RecordMetadata> send(org.apache.kafka.clients.producer.Producer producerInterface, String topic, Integer partition, String key, Payload payload, AfterCall afterCall) throws IOException {
        return producerInterface.send(new ProducerRecord<>(topic, partition, key, OBJECT_MAPPER.writeValueAsString(payload)), (metadata, exception) -> {
            if (afterCall != null)
                afterCall.after(topic, metadata.partition(), metadata.offset(), exception, payload, producerProperties);
        });
    }


    /**
     * Validate the Payload for expected elements.
     *
     * @param payload
     * @throws InvalidPayloadException
     */
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


    /**
     * Method to fetch PartitionInfo list for a given topic.
     *
     * @param topic
     * @return List of PartitionInfo
     * @throws IOException
     */
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

    /**
     * Method to return KafkaProducer metrics.
     *
     * @return JSON representation of all metrics.
     * @throws IOException
     */
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

    /**
     * Support method to wrap IOException.
     *
     * @param exception
     * @throws IOException
     */
    private void throwIOException(String exception) throws IOException {
        throw new IOException(exception);
    }
}
