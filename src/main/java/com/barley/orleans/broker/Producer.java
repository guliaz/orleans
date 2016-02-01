package com.barley.orleans.broker;

import com.barley.orleans.structure.Payload;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public final class Producer {

    private static KafkaProducer<String, String> kafkaProducer = null;
    private static Producer producer = null;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static boolean isProducerAlive = false;

    private Producer(Properties properties) {
        if (properties != null)
            kafkaProducer = new KafkaProducer<String, String>(properties);
        isProducerAlive = true;
    }

    public static Producer producer(Properties properties) {
        if (!isProducerAlive || producer == null || kafkaProducer == null) {
            producer = new Producer(properties);
        }
        return producer;
    }

    public Future<RecordMetadata> produce(String topic, Payload payload) throws IOException {
        return kafkaProducer.send(new ProducerRecord<String, String>(topic, OBJECT_MAPPER.writeValueAsString(payload)));
    }
}
