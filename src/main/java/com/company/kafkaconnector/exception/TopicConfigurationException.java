package com.company.kafkaconnector.exception;

public class TopicConfigurationException extends ConnectorException {
    private final String configProperty;

    public TopicConfigurationException(String message, String topicName, String configProperty) {
        super(message, topicName, null);
        this.configProperty = configProperty;
    }

    public TopicConfigurationException(String message, String topicName, String configProperty, Throwable cause) {
        super(message, topicName, null, cause);
        this.configProperty = configProperty;
    }

    public String getConfigProperty() {
        return configProperty;
    }
}