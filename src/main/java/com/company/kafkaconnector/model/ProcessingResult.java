package com.company.kafkaconnector.model;

import java.time.LocalDateTime;

public class ProcessingResult {
    private boolean success;
    private String topicName;
    private String errorMessage;
    private int recordsProcessed;
    private int recordsFailed;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private long processingTimeMs;

    // Constructors
    public ProcessingResult() {}

    public ProcessingResult(boolean success, String topicName) {
        this.success = success;
        this.topicName = topicName;
        this.startTime = LocalDateTime.now();
    }

    // Getters and Setters
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public int getRecordsProcessed() {
        return recordsProcessed;
    }

    public void setRecordsProcessed(int recordsProcessed) {
        this.recordsProcessed = recordsProcessed;
    }

    public int getRecordsFailed() {
        return recordsFailed;
    }

    public void setRecordsFailed(int recordsFailed) {
        this.recordsFailed = recordsFailed;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public long getProcessingTimeMs() {
        return processingTimeMs;
    }

    public void setProcessingTimeMs(long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }

    public void markComplete() {
        this.endTime = LocalDateTime.now();
        if (startTime != null) {
            this.processingTimeMs = java.time.Duration.between(startTime, endTime).toMillis();
        }
    }
}