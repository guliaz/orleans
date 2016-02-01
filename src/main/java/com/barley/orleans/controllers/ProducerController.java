package com.barley.orleans.controllers;

import com.barley.orleans.broker.Producer;
import com.barley.orleans.properties.ProducerProperties;
import com.barley.orleans.structure.Payload;
import com.barley.orleans.structure.Response;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.util.concurrent.Future;

@RequestMapping("v1/produce")
public class ProducerController {

    @Autowired
    private ProducerProperties producerProperties;

    private Producer producer;

    public ProducerController() {
        producer = Producer.producer(producerProperties.properties());
    }

    @RequestMapping("/{topic}")
    public Response produce(@PathVariable(value = "topic") String topic, Payload payload) {
        try {
            Future<RecordMetadata> future = producer.produce(topic, payload);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return new Response();
    }

}
