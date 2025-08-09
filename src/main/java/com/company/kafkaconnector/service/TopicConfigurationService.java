package com.company.kafkaconnector.service;

import com.company.kafkaconnector.config.ConnectorConfiguration;
import com.company.kafkaconnector.model.TopicConfig;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class TopicConfigurationService {
    
    private final Map<String, TopicConfig> topicConfigs;
    
    public TopicConfigurationService(ConnectorConfiguration connectorConfiguration) {
        this.topicConfigs = connectorConfiguration.getTopics();
    }
    
    public TopicConfig getConfigForTopic(String topicName) {
        for (TopicConfig config : topicConfigs.values()) {
            if (config.getKafkaTopic().equals(topicName)) {
                return config;
            }
        }
        return null;
    }
}