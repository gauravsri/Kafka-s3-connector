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
    
    public boolean isRetriable() {
        String message = getMessage() != null ? getMessage().toLowerCase() : "";
        
        // Network/timeout errors are usually retriable
        if (message.contains("timeout") || message.contains("connection") || message.contains("network")) {
            return true;
        }
        
        // S3 throttling is retriable
        if (message.contains("throttle") || message.contains("rate limit")) {
            return true;
        }
        
        // Schema validation errors are usually not retriable
        if (message.contains("schema") || message.contains("validation")) {
            return false;
        }
        
        // Default to retriable for unknown errors
        return true;
    }
}