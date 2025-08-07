package com.company.kafkaconnector.config;

import com.company.kafkaconnector.model.TopicConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "connector")
public class ConnectorConfiguration {
    private Map<String, TopicConfig> topics = new HashMap<>();

    // Constructors
    public ConnectorConfiguration() {}

    public ConnectorConfiguration(Map<String, TopicConfig> topics) {
        this.topics = topics;
    }

    // Getters and Setters
    public Map<String, TopicConfig> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, TopicConfig> topics) {
        this.topics = topics;
    }

    // Helper methods
    public TopicConfig getTopicConfig(String topicConfigKey) {
        return topics.get(topicConfigKey);
    }

    public boolean hasTopicConfig(String topicConfigKey) {
        return topics.containsKey(topicConfigKey);
    }

    public void addTopicConfig(String topicConfigKey, TopicConfig topicConfig) {
        this.topics.put(topicConfigKey, topicConfig);
    }

    public TopicConfig findByKafkaTopic(String kafkaTopicName) {
        return topics.values().stream()
                .filter(config -> config.getKafkaTopic().equals(kafkaTopicName))
                .findFirst()
                .orElse(null);
    }
}