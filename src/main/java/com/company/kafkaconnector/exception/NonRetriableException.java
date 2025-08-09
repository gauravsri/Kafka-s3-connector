package com.company.kafkaconnector.exception;

/**
 * Exception that indicates a permanent failure that should NOT be retried.
 * 
 * <p>When this exception is thrown, the connector will:
 * <ul>
 *   <li>Acknowledge the Kafka message immediately</li>
 *   <li>Send message directly to DLQ without retries</li>
 *   <li>Continue processing other messages</li>
 * </ul>
 */
public class NonRetriableException extends RuntimeException {
    
    public NonRetriableException(String message) {
        super(message);
    }
    
    public NonRetriableException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Creates a non-retriable exception for schema validation failures
     */
    public static NonRetriableException schemaValidationFailed(String details) {
        return new NonRetriableException("Schema validation failed: " + details);
    }
    
    /**
     * Creates a non-retriable exception for malformed messages
     */
    public static NonRetriableException malformedMessage(String details, Throwable cause) {
        return new NonRetriableException("Malformed message: " + details, cause);
    }
    
    /**
     * Creates a non-retriable exception for configuration errors
     */
    public static NonRetriableException configurationError(String details) {
        return new NonRetriableException("Configuration error: " + details);
    }
}