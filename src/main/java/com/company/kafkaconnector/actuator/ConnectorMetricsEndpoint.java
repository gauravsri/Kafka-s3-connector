package com.company.kafkaconnector.actuator;

import com.company.kafkaconnector.metrics.ConnectorMetrics;
import com.company.kafkaconnector.metrics.ConnectorMetricsSnapshot;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * REST endpoint for connector metrics and health information
 * Custom actuator-style endpoints for monitoring
 */
@RestController
@RequestMapping("/actuator/connector")
@RequiredArgsConstructor
@Slf4j
public class ConnectorMetricsEndpoint {
    
    private final ConnectorMetrics connectorMetrics;
    private final Instant startupTime = Instant.now();
    
    @GetMapping("/health")
    public Map<String, Object> health() {
        try {
            ConnectorMetricsSnapshot snapshot = connectorMetrics.getSnapshot();
            
            Map<String, Object> health = new HashMap<>();
            
            // Determine overall status
            String status = determineHealthStatus(snapshot);
            health.put("status", status);
            health.put("timestamp", LocalDateTime.now());
            
            // Add detailed components
            Map<String, Object> components = new HashMap<>();
            components.put("processing", createProcessingHealth(snapshot));
            components.put("storage", createStorageHealth(snapshot));
            components.put("performance", createPerformanceHealth(snapshot));
            
            health.put("components", components);
            health.put("details", createHealthDetails(snapshot));
            
            return health;
            
        } catch (Exception e) {
            log.error("Health check failed", e);
            return Map.of(
                "status", "DOWN",
                "timestamp", LocalDateTime.now(),
                "error", e.getMessage()
            );
        }
    }
    
    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        try {
            ConnectorMetricsSnapshot snapshot = connectorMetrics.getSnapshot();
            
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("timestamp", LocalDateTime.now());
            metrics.put("uptime-seconds", Instant.now().getEpochSecond() - startupTime.getEpochSecond());
            
            // Processing metrics
            Map<String, Object> processing = new HashMap<>();
            processing.put("records-processed", snapshot.getRecordsProcessed());
            processing.put("records-failed", snapshot.getRecordsFailed());
            processing.put("failure-rate", snapshot.getFailureRate());
            processing.put("schema-validation-failures", snapshot.getSchemaValidationFailures());
            
            // Storage metrics
            Map<String, Object> storage = new HashMap<>();
            storage.put("s3-writes", snapshot.getS3Writes());
            storage.put("s3-write-failures", snapshot.getS3WriteFailures());
            storage.put("s3-failure-rate", snapshot.getS3WriteFailureRate());
            storage.put("current-file-size", snapshot.getCurrentFileSize());
            
            // Performance metrics
            Map<String, Object> performance = new HashMap<>();
            performance.put("avg-processing-time-ms", snapshot.getAvgRecordProcessingTime());
            performance.put("avg-s3-write-time-ms", snapshot.getAvgS3WriteTime());
            performance.put("avg-schema-validation-time-ms", snapshot.getAvgSchemaValidationTime());
            
            // System metrics
            Map<String, Object> system = new HashMap<>();
            system.put("active-connections", snapshot.getActiveConnections());
            system.put("buffered-records", snapshot.getBufferedRecords());
            system.put("dlq-messages", snapshot.getDlqMessages());
            system.put("circuit-breaker-opens", snapshot.getCircuitBreakerOpens());
            
            metrics.put("processing", processing);
            metrics.put("storage", storage);
            metrics.put("performance", performance);
            metrics.put("system", system);
            
            return metrics;
            
        } catch (Exception e) {
            log.error("Failed to get metrics", e);
            return Map.of(
                "error", e.getMessage(),
                "timestamp", LocalDateTime.now()
            );
        }
    }
    
    @GetMapping("/info")
    public Map<String, Object> info() {
        Map<String, Object> info = new HashMap<>();
        
        // Connector information
        Map<String, Object> connector = new HashMap<>();
        connector.put("name", "S3 Delta Lake Sink Connector");
        connector.put("version", "1.0.0");
        connector.put("type", "Kafka Connect Sink Connector");
        connector.put("startup-time", LocalDateTime.ofInstant(startupTime, ZoneId.systemDefault()));
        connector.put("uptime-seconds", Instant.now().getEpochSecond() - startupTime.getEpochSecond());
        
        // Features
        Map<String, Object> features = new HashMap<>();
        features.put("enterprise-patterns", true);
        features.put("structured-logging", true);
        features.put("comprehensive-metrics", true);
        features.put("correlation-tracking", true);
        features.put("circuit-breaker", true);
        features.put("dead-letter-queue", true);
        features.put("retry-mechanism", true);
        features.put("performance-monitoring", true);
        
        info.put("connector", connector);
        info.put("features", features);
        
        return info;
    }
    
    private String determineHealthStatus(ConnectorMetricsSnapshot snapshot) {
        double failureRate = snapshot.getFailureRate();
        double s3WriteFailureRate = snapshot.getS3WriteFailureRate();
        
        if (failureRate > 0.1 || s3WriteFailureRate > 0.1) {
            return "DOWN";
        } else if (failureRate > 0.05 || s3WriteFailureRate > 0.05) {
            return "DEGRADED";
        } else {
            return "UP";
        }
    }
    
    private Map<String, Object> createProcessingHealth(ConnectorMetricsSnapshot snapshot) {
        Map<String, Object> processing = new HashMap<>();
        double failureRate = snapshot.getFailureRate();
        
        processing.put("status", failureRate > 0.1 ? "DOWN" : failureRate > 0.05 ? "DEGRADED" : "UP");
        processing.put("failure-rate", failureRate);
        processing.put("records-processed", snapshot.getRecordsProcessed());
        processing.put("records-failed", snapshot.getRecordsFailed());
        
        return processing;
    }
    
    private Map<String, Object> createStorageHealth(ConnectorMetricsSnapshot snapshot) {
        Map<String, Object> storage = new HashMap<>();
        double s3FailureRate = snapshot.getS3WriteFailureRate();
        
        storage.put("status", s3FailureRate > 0.1 ? "DOWN" : s3FailureRate > 0.05 ? "DEGRADED" : "UP");
        storage.put("s3-failure-rate", s3FailureRate);
        storage.put("s3-writes", snapshot.getS3Writes());
        storage.put("s3-write-failures", snapshot.getS3WriteFailures());
        
        return storage;
    }
    
    private Map<String, Object> createPerformanceHealth(ConnectorMetricsSnapshot snapshot) {
        Map<String, Object> performance = new HashMap<>();
        
        boolean slowProcessing = snapshot.getAvgRecordProcessingTime() > 100;
        boolean slowS3Writes = snapshot.getAvgS3WriteTime() > 5000;
        
        performance.put("status", (slowProcessing || slowS3Writes) ? "DEGRADED" : "UP");
        performance.put("avg-processing-time-ms", snapshot.getAvgRecordProcessingTime());
        performance.put("avg-s3-write-time-ms", snapshot.getAvgS3WriteTime());
        performance.put("slow-processing-detected", slowProcessing);
        performance.put("slow-s3-writes-detected", slowS3Writes);
        
        return performance;
    }
    
    private Map<String, Object> createHealthDetails(ConnectorMetricsSnapshot snapshot) {
        Map<String, Object> details = new HashMap<>();
        
        details.put("active-connections", snapshot.getActiveConnections());
        details.put("buffered-records", snapshot.getBufferedRecords());
        details.put("dlq-messages", snapshot.getDlqMessages());
        details.put("circuit-breaker-opens", snapshot.getCircuitBreakerOpens());
        details.put("current-file-size", snapshot.getCurrentFileSize());
        
        return details;
    }
}