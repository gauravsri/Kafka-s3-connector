package com.company.kafkaconnector.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class TopicConfig {
    @NotBlank
    private String kafkaTopic;
    
    @NotBlank
    private String schemaFile;
    
    @Valid
    @NotNull
    private DestinationConfig destination;
    
    @Valid
    @NotNull
    private ProcessingConfig processing;

    // Constructors
    public TopicConfig() {
        this.destination = new DestinationConfig();
        this.processing = new ProcessingConfig();
    }

    public TopicConfig(String kafkaTopic, String schemaFile, DestinationConfig destination, ProcessingConfig processing) {
        this.kafkaTopic = kafkaTopic;
        this.schemaFile = schemaFile;
        this.destination = destination;
        this.processing = processing;
    }

    // Getters and Setters
    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }

    public DestinationConfig getDestination() {
        return destination;
    }

    public void setDestination(DestinationConfig destination) {
        this.destination = destination;
    }

    public ProcessingConfig getProcessing() {
        return processing;
    }

    public void setProcessing(ProcessingConfig processing) {
        this.processing = processing;
    }
}