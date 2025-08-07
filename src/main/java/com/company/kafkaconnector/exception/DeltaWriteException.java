package com.company.kafkaconnector.exception;

public class DeltaWriteException extends ConnectorException {
    private final String tablePath;
    private final int batchSize;

    public DeltaWriteException(String message, String tablePath, int batchSize) {
        super(message);
        this.tablePath = tablePath;
        this.batchSize = batchSize;
    }

    public DeltaWriteException(String message, String tablePath, int batchSize, Throwable cause) {
        super(message, cause);
        this.tablePath = tablePath;
        this.batchSize = batchSize;
    }

    public DeltaWriteException(String message, String topicName, String correlationId, 
                             String tablePath, int batchSize) {
        super(message, topicName, correlationId);
        this.tablePath = tablePath;
        this.batchSize = batchSize;
    }

    public DeltaWriteException(String message, String topicName, String correlationId, 
                             String tablePath, int batchSize, Throwable cause) {
        super(message, topicName, correlationId, cause);
        this.tablePath = tablePath;
        this.batchSize = batchSize;
    }

    public String getTablePath() {
        return tablePath;
    }

    public int getBatchSize() {
        return batchSize;
    }
}