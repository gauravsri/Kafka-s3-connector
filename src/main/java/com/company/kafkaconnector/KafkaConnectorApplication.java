package com.company.kafkaconnector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaConnectorApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(KafkaConnectorApplication.class, args);
    }
}