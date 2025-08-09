package com.company.kafkaconnector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
@EnableRetry
public class KafkaConnectorApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(KafkaConnectorApplication.class, args);
    }
}