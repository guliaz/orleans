package com.barley.orleans.properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Configuration class to serve properties for producer.
 */
@Configuration
public class ProducerProperties {

    @Value("${bootstrap.servers}")
    String bootstrapServers;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public Properties properties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", getBootstrapServers());
        return properties;
    }

}
