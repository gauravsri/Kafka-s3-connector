package com.company.kafkaconnector.exception;

public class CobExtractionException extends RuntimeException {
    
    public CobExtractionException(String message) {
        super(message);
    }
    
    public CobExtractionException(String message, Throwable cause) {
        super(message, cause);
    }
}