package com.company.kafkaconnector.exception;

public class SchemaValidationException extends ConnectorException {
    private final String schemaFile;
    private final String invalidMessage;

    public SchemaValidationException(String message, String schemaFile, String invalidMessage) {
        super(message);
        this.schemaFile = schemaFile;
        this.invalidMessage = invalidMessage;
    }

    public SchemaValidationException(String message, String schemaFile, String invalidMessage, Throwable cause) {
        super(message, cause);
        this.schemaFile = schemaFile;
        this.invalidMessage = invalidMessage;
    }

    public SchemaValidationException(String message, String topicName, String correlationId, 
                                   String schemaFile, String invalidMessage) {
        super(message, topicName, correlationId);
        this.schemaFile = schemaFile;
        this.invalidMessage = invalidMessage;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public String getInvalidMessage() {
        return invalidMessage;
    }
}