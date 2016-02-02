package com.barley.orleans.properties;

import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Configuration class to serve properties for producer.
 */
@RefreshScope
@Configuration
public class ProducerProperties {

    public Properties properties() {
        return new Properties();
    }

}
