package com.company.kafkaconnector.exception;

/**
 * Exception that indicates a transient failure that should be retried.
 * Similar to Kafka Connect's RetriableException pattern.
 * 
 * <p>When this exception is thrown, the connector will:
 * <ul>
 *   <li>NOT acknowledge the Kafka message</li>
 *   <li>Apply exponential backoff retry logic</li>
 *   <li>Eventually send to DLQ after max retries exceeded</li>
 * </ul>
 */
public class RetriableException extends RuntimeException {
    
    public RetriableException(String message) {
        super(message);
    }
    
    public RetriableException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Creates a retriable exception for transient network issues
     */
    public static RetriableException networkError(String operation, Throwable cause) {
        return new RetriableException("Network error during " + operation + ": " + cause.getMessage(), cause);
    }
    
    /**
     * Creates a retriable exception for temporary service unavailability
     */
    public static RetriableException serviceUnavailable(String service, Throwable cause) {
        return new RetriableException("Service temporarily unavailable: " + service, cause);
    }
    
    /**
     * Creates a retriable exception for resource constraints
     */
    public static RetriableException resourceConstraint(String resource, Throwable cause) {
        return new RetriableException("Resource constraint: " + resource, cause);
    }
}