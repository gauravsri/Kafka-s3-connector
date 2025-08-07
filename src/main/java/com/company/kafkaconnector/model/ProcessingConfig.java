package com.company.kafkaconnector.model;

import jakarta.validation.constraints.Min;

public class ProcessingConfig {
    @Min(1)
    private int batchSize = 1000;
    
    @Min(1)
    private int flushInterval = 60; // seconds
    
    @Min(0)
    private int maxRetries = 3;

    // Constructors
    public ProcessingConfig() {}

    public ProcessingConfig(int batchSize, int flushInterval, int maxRetries) {
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetries;
    }

    // Getters and Setters
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
}