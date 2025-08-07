package com.company.kafkaconnector.metrics;

import lombok.Builder;
import lombok.Data;

/**
 * Snapshot of connector metrics at a point in time
 * Following Value Object pattern with Lombok
 */
@Data
@Builder
public class ConnectorMetricsSnapshot {
    private final double recordsProcessed;
    private final double recordsFailed;
    private final double s3Writes;
    private final double s3WriteFailures;
    private final double schemaValidationFailures;
    private final double dlqMessages;
    private final double circuitBreakerOpens;
    private final long activeConnections;
    private final long bufferedRecords;
    private final long currentFileSize;
    private final double avgRecordProcessingTime;
    private final double avgS3WriteTime;
    private final double avgSchemaValidationTime;
    
    public double getFailureRate() {
        if (recordsProcessed == 0) return 0.0;
        return recordsFailed / recordsProcessed;
    }
    
    public double getS3WriteFailureRate() {
        if (s3Writes == 0) return 0.0;
        return s3WriteFailures / s3Writes;
    }
}