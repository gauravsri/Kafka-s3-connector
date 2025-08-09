package com.company.kafkaconnector.exception;

public class MessageParsingException extends RuntimeException {
    
    public MessageParsingException(String message) {
        super(message);
    }
    
    public MessageParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}