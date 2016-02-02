package com.barley.orleans.interfaces;

import com.barley.orleans.structure.Payload;

import java.util.Properties;

/**
 * An interface which user can implement to allow API to invoke after the call of a method is complete.
 */
public interface AfterCall {

    /**
     * Method which client can implement to be executed after call to a given method is complete
     *
     * @param topic      - will be present to tell which topic messages was tried to be produced or is consumed from. Should be present all the time.
     * @param partition  - if successful produced to kafka or message consumed from kafka, it will be present. If produce failed, will be null.
     * @param offset     - if successful produced to kafka or message consumed from kafka, it will be present. If produce failed, will be null.
     * @param exception  - if any exception when producing or consuming from kafkam it will be present.
     * @param payload    - Payload which was tried to be produced or which was consumed from kafka topic.
     * @param properties - Any properties available to producer or consumer.
     */
    public void after(String topic, Integer partition, Long offset, Exception exception, Payload payload, Properties properties);
}
