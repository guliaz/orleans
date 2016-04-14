package com.barley.orleans.broker;

import com.barley.orleans.exceptions.InvalidPayloadException;
import com.barley.orleans.interfaces.AfterCall;
import com.barley.orleans.structure.Payload;
import com.barley.orleans.structure.Response;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
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
 * <p>
 * This Producer class is a
 * </p>
 */
public final class Producer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private boolean isProducerAlive = false;
    private boolean isMockProducer = false;
    private Properties producerProperties = null;

    /**
     * Producer private constructor for internal use
     *
     * @param properties Properties needed for Producer
     * @param isMock     is this producer a mock one (Only for testing purposes, should be always false in production environment)
     */
    public Producer(Properties properties, boolean isMock) {
        this.producerProperties = properties;
        if (!isMock)
            this.kafkaProducer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        else {
            this.kafkaProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
            this.isMockProducer = true;
        }
        this.isProducerAlive = true;
    }

    /**
     * method to flush the messages in sender's queue.
     */
    public void flush() {
        if (isProducerAlive) {
            kafkaProducer.flush();
        }
    }

    /**
     * Closing the underlying producer. If closing, I think flushing before would be better.
     */
    public void close() {
        if (isProducerAlive) {
            kafkaProducer.close();
            isMockProducer = false;
            isProducerAlive = false;
        }
    }

    /**
     * Tells if this producer a mock one.
     *
     * @return boolean
     */
    public boolean isMockProducer() {
        return isMockProducer;
    }

    /**
     * Tells if a kafka producer created yet or not.
     *
     * @return boolean
     */
    public boolean isProducerAlive() {
        return isProducerAlive;
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
    private Response produce(final String topic, Integer partition, String key, final Payload payload, final AfterCall afterCall) {
        final Response response = new Response();
        try {
            if (topic == null || topic.length() == 0)
                throw new InvalidPayloadException("Topic cannot be null or empty");
            validateInput(payload);
            final Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<>(topic, partition, key, OBJECT_MAPPER.writeValueAsString(payload)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (afterCall != null)
                        afterCall.after(topic, metadata.partition(), metadata.offset(), exception, payload, producerProperties);
                }
            });
            RecordMetadata recordMetadata = future.get(100, TimeUnit.MILLISECONDS);
            response.setOffset(recordMetadata.offset());
            response.setPartition(recordMetadata.partition());
        } catch (IOException e) {
            logger.error("Error while producing record to kafka for topic: " + topic + " with payload: " + payload, e);
            response.addError(e.getLocalizedMessage());
        } catch (InterruptedException e) {
            logger.error("Error while producing record to kafka for topic: " + topic + " with payload: " + payload, e);
            response.addError(e.getLocalizedMessage());
        } catch (ExecutionException e) {
            logger.error("Error while producing record to kafka for topic: " + topic + " with payload: " + payload, e);
            response.addError(e.getLocalizedMessage());
        } catch (TimeoutException e) {
            logger.error("Error while producing record to kafka for topic: " + topic + " with payload: " + payload, e);
            response.addError(e.getLocalizedMessage());
        } catch (InvalidPayloadException e) {
            logger.error("Error while producing record to kafka for topic: " + topic + " with payload: " + payload, e);
            response.addError(e.getLocalizedMessage());
        }
        return response;
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
        if (kafkaProducer != null)
            partitionInfoList = kafkaProducer.partitionsFor(topic);
        else
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
        if (kafkaProducer != null)
            metrics = OBJECT_MAPPER.writeValueAsString(kafkaProducer.metrics());
        else
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
