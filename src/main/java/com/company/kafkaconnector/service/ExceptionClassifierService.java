package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.NonRetriableException;
import com.company.kafkaconnector.exception.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

/**
 * Service that classifies exceptions as retriable or non-retriable.
 * Based on Kafka Connect's exception classification pattern.
 */
@Service
public class ExceptionClassifierService {
    
    private static final Logger logger = LoggerFactory.getLogger(ExceptionClassifierService.class);
    
    /**
     * Determines if an exception should be retried or not.
     * 
     * @param exception the exception to classify
     * @return true if the exception is retriable, false otherwise
     */
    public boolean isRetriable(Throwable exception) {
        // Already classified exceptions
        if (exception instanceof RetriableException) {
            return true;
        }
        if (exception instanceof NonRetriableException) {
            return false;
        }
        
        // Classify based on exception type and characteristics
        return classifyException(exception);
    }
    
    /**
     * Wraps an exception in the appropriate retriable/non-retriable wrapper.
     * 
     * @param exception the original exception
     * @param operation the operation that failed
     * @return the wrapped exception
     */
    public RuntimeException wrapException(Throwable exception, String operation) {
        if (exception instanceof RetriableException || exception instanceof NonRetriableException) {
            return (RuntimeException) exception;
        }
        
        if (isRetriable(exception)) {
            return new RetriableException("Retriable failure in " + operation + ": " + exception.getMessage(), exception);
        } else {
            return new NonRetriableException("Non-retriable failure in " + operation + ": " + exception.getMessage(), exception);
        }
    }
    
    private boolean classifyException(Throwable exception) {
        // Network and I/O related exceptions (retriable)
        if (exception instanceof ConnectException ||
            exception instanceof SocketTimeoutException ||
            exception instanceof UnknownHostException ||
            exception instanceof TimeoutException ||
            exception instanceof IOException) {
            
            logger.debug("Classified as retriable: {} - {}", 
                exception.getClass().getSimpleName(), exception.getMessage());
            return true;
        }
        
        // AWS/S3 specific exceptions (retriable)
        String exceptionName = exception.getClass().getSimpleName();
        if (exceptionName.contains("ServiceException") ||
            exceptionName.contains("ThrottlingException") ||
            exceptionName.contains("InternalServerError") ||
            exceptionName.contains("ServiceUnavailableException")) {
            
            logger.debug("Classified as retriable AWS exception: {} - {}", 
                exceptionName, exception.getMessage());
            return true;
        }
        
        // Delta Lake specific exceptions (retriable for certain cases)
        if (exceptionName.contains("ConcurrentModificationException") ||
            exceptionName.contains("FileNotFoundException")) {
            
            logger.debug("Classified as retriable Delta exception: {} - {}", 
                exceptionName, exception.getMessage());
            return true;
        }
        
        // Check error message for retriable indicators
        String message = exception.getMessage();
        if (message != null) {
            String lowerMessage = message.toLowerCase();
            if (lowerMessage.contains("timeout") ||
                lowerMessage.contains("connection") ||
                lowerMessage.contains("unavailable") ||
                lowerMessage.contains("throttl") ||
                lowerMessage.contains("rate limit") ||
                lowerMessage.contains("too many requests")) {
                
                logger.debug("Classified as retriable based on message: {}", message);
                return true;
            }
        }
        
        // HTTP status code based classification
        if (exception.getCause() != null) {
            String causeMessage = exception.getCause().getMessage();
            if (causeMessage != null && isRetriableHttpStatus(causeMessage)) {
                logger.debug("Classified as retriable based on HTTP status in cause: {}", causeMessage);
                return true;
            }
        }
        
        // Default to non-retriable for safety
        logger.debug("Classified as non-retriable: {} - {}", 
            exception.getClass().getSimpleName(), exception.getMessage());
        return false;
    }
    
    private boolean isRetriableHttpStatus(String message) {
        // Look for HTTP status codes that indicate retriable errors
        return message.contains("502") ||  // Bad Gateway
               message.contains("503") ||  // Service Unavailable
               message.contains("504") ||  // Gateway Timeout
               message.contains("429") ||  // Too Many Requests
               message.contains("408") ||  // Request Timeout
               message.contains("500");    // Internal Server Error
    }
    
    /**
     * Get a human-readable classification reason for debugging
     */
    public String getClassificationReason(Throwable exception) {
        if (exception instanceof RetriableException) {
            return "Already marked as retriable";
        }
        if (exception instanceof NonRetriableException) {
            return "Already marked as non-retriable";
        }
        
        if (isRetriable(exception)) {
            return "Classified as retriable: " + getRetriableReason(exception);
        } else {
            return "Classified as non-retriable: permanent failure or unknown error type";
        }
    }
    
    private String getRetriableReason(Throwable exception) {
        String className = exception.getClass().getSimpleName();
        String message = exception.getMessage();
        
        if (className.contains("Connect") || className.contains("Timeout") || className.contains("IO")) {
            return "Network/IO issue";
        }
        if (className.contains("Service") || className.contains("Throttling")) {
            return "Service availability issue";
        }
        if (message != null && (message.toLowerCase().contains("timeout") || message.toLowerCase().contains("connection"))) {
            return "Message indicates transient issue";
        }
        
        return "Heuristic classification";
    }
}