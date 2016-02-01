package com.barley.orleans.properties;

import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@RefreshScope
@Configuration
public class ProducerProperties {

    public Properties properties() {
        return new Properties();
    }

}
