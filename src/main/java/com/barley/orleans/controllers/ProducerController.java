package com.barley.orleans.controllers;

import com.barley.orleans.broker.Producer;
import com.barley.orleans.properties.ProducerProperties;
import com.barley.orleans.structure.Payload;
import com.barley.orleans.structure.Response;
import com.barley.orleans.structure.ResponseList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rest controller to expose method to produce list of Payloads to Kafka.
 * It uses
 */
@RestController
@RequestMapping("v1/produce")
public class ProducerController implements ApplicationListener<ApplicationContextEvent> {

    @Autowired
    private ProducerProperties producerProperties;

    private static Producer producer;

    @RequestMapping(value = "/{topic}", method = {RequestMethod.POST}, produces = {MediaType.APPLICATION_JSON_VALUE}, consumes = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<ResponseList> produce(@PathVariable(value = "topic") String topic, @RequestBody List<Payload> payloads) {
        final ResponseList responseList = new ResponseList();
        responseList.setResponses(payloads.parallelStream().map(payload -> {
            final Response response = producer.produce(topic, payload);
            if (response.getErrors().size() > 0)
                responseList.setStatus(HttpStatus.BAD_REQUEST.value());
            return response;
        }).collect(Collectors.toList()));
        return new ResponseEntity<>(responseList, HttpStatus.valueOf(responseList.getStatus()));
    }


    @RequestMapping(value = "/{metrics}", method = {RequestMethod.GET}, produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<String> metrics() {
        String metrics = "{}";
        HttpStatus httpStatus = HttpStatus.OK;
        try {
            metrics = producer.metrics();
        } catch (IOException ioe) {
            httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        return new ResponseEntity<>(metrics, httpStatus);
    }

    private void initProducer() {
        if (!producer.isProducerAlive() && producerProperties != null)
            producer = Producer.producer(producerProperties.properties());
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if ((event instanceof ContextStartedEvent || event instanceof ContextRefreshedEvent) && producer == null) {
            initProducer();
        }
    }
}
