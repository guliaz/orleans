package com.barley.orleans.broker;

import com.barley.orleans.structure.PayloadBuilder;
import com.barley.orleans.structure.Response;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * Test class for Producer
 */
public class ProducerTest {

    private static Producer producer = null;
    private static PayloadBuilder payloadBuilder = null;

    @BeforeClass
    public static void setUp() {
        producer = Producer.producer(null, true);
        payloadBuilder = PayloadBuilder.aPayload().withClient("TEST").withIpAddress("10.0.0.1").withSchemaId("").withUuid(UUID.randomUUID().toString());
    }

    @Test
    public void testDummyPayload() throws Exception {
        Response response = producer.produce("topic", payloadBuilder.withData("mydata").build());
        assertTrue(response.getOffset() != null && response.getOffset() >= 0);
    }

    @Test
    public void testNullPayload() throws Exception {
        Response response = producer.produce("topic", null);
        assertTrue(response.getErrors().size() > 0 && response.getErrors().get(0).equalsIgnoreCase("Payload cannot be null"));
    }

    @Test
    public void testNullTopic() throws Exception {
        Response response = producer.produce(null, null);
        assertTrue(response.getErrors().size() > 0 && response.getErrors().get(0).equalsIgnoreCase("Topic cannot be null or empty"));
    }
}