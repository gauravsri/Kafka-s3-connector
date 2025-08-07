package com.company.kafkaconnector.exception;

public class ConnectorException extends RuntimeException {
    private final String topicName;
    private final String correlationId;

    public ConnectorException(String message) {
        super(message);
        this.topicName = null;
        this.correlationId = null;
    }

    public ConnectorException(String message, Throwable cause) {
        super(message, cause);
        this.topicName = null;
        this.correlationId = null;
    }

    public ConnectorException(String message, String topicName, String correlationId) {
        super(message);
        this.topicName = topicName;
        this.correlationId = correlationId;
    }

    public ConnectorException(String message, String topicName, String correlationId, Throwable cause) {
        super(message, cause);
        this.topicName = topicName;
        this.correlationId = correlationId;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getCorrelationId() {
        return correlationId;
    }
}