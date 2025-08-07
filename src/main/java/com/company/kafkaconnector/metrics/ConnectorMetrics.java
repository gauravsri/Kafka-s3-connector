package com.company.kafkaconnector.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive metrics for Kafka S3 Delta Connector
 * Following Micrometer patterns and Single Responsibility Principle
 */
@Slf4j
@Component
public class ConnectorMetrics {
    
    private final MeterRegistry meterRegistry;
    
    // Counters for tracking events
    private final Counter recordsProcessedCounter;
    private final Counter recordsFailedCounter;
    private final Counter s3WritesCounter;
    private final Counter s3WriteFailuresCounter;
    private final Counter schemaValidationFailuresCounter;
    private final Counter dlqMessagesCounter;
    private final Counter circuitBreakerOpenCounter;
    
    // Timers for measuring latencies
    private final Timer recordProcessingTimer;
    private final Timer s3WriteTimer;
    private final Timer schemaValidationTimer;
    
    // Gauges for current state
    private final AtomicLong activeConnections = new AtomicLong(0);
    private final AtomicLong bufferedRecords = new AtomicLong(0);
    private final AtomicLong currentFileSize = new AtomicLong(0);
    
    // Per-topic metrics
    private final ConcurrentHashMap<String, TopicMetrics> topicMetrics = new ConcurrentHashMap<>();
    
    public ConnectorMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize counters
        this.recordsProcessedCounter = Counter.builder("kafka_connector_records_processed_total")
            .description("Total number of records processed")
            .register(meterRegistry);
            
        this.recordsFailedCounter = Counter.builder("kafka_connector_records_failed_total")
            .description("Total number of records that failed processing")
            .register(meterRegistry);
            
        this.s3WritesCounter = Counter.builder("kafka_connector_s3_writes_total")
            .description("Total number of successful S3 writes")
            .register(meterRegistry);
            
        this.s3WriteFailuresCounter = Counter.builder("kafka_connector_s3_write_failures_total")
            .description("Total number of failed S3 writes")
            .register(meterRegistry);
            
        this.schemaValidationFailuresCounter = Counter.builder("kafka_connector_schema_validation_failures_total")
            .description("Total number of schema validation failures")
            .register(meterRegistry);
            
        this.dlqMessagesCounter = Counter.builder("kafka_connector_dlq_messages_total")
            .description("Total number of messages sent to dead letter queue")
            .register(meterRegistry);
            
        this.circuitBreakerOpenCounter = Counter.builder("kafka_connector_circuit_breaker_open_total")
            .description("Total number of times circuit breaker opened")
            .register(meterRegistry);
        
        // Initialize timers
        this.recordProcessingTimer = Timer.builder("kafka_connector_record_processing_duration")
            .description("Time taken to process a single record")
            .register(meterRegistry);
            
        this.s3WriteTimer = Timer.builder("kafka_connector_s3_write_duration")
            .description("Time taken to write to S3")
            .register(meterRegistry);
            
        this.schemaValidationTimer = Timer.builder("kafka_connector_schema_validation_duration")
            .description("Time taken for schema validation")
            .register(meterRegistry);
        
        // Initialize gauges
        Gauge.builder("kafka_connector_active_connections", this, metrics -> metrics.activeConnections.get())
            .description("Number of active connections")
            .register(meterRegistry);
            
        Gauge.builder("kafka_connector_buffered_records", this, metrics -> metrics.bufferedRecords.get())
            .description("Number of records currently buffered")
            .register(meterRegistry);
            
        Gauge.builder("kafka_connector_current_file_size_bytes", this, metrics -> metrics.currentFileSize.get())
            .description("Current file size being written in bytes")
            .register(meterRegistry);
    }
    
    // Counter methods
    public void incrementRecordsProcessed() {
        recordsProcessedCounter.increment();
    }
    
    public void incrementRecordsProcessed(String topic) {
        recordsProcessedCounter.increment();
        getTopicMetrics(topic).incrementRecordsProcessed();
    }
    
    public void incrementRecordsFailed() {
        recordsFailedCounter.increment();
    }
    
    public void incrementRecordsFailed(String topic) {
        recordsFailedCounter.increment();
        getTopicMetrics(topic).incrementRecordsFailed();
    }
    
    public void incrementS3Writes() {
        s3WritesCounter.increment();
    }
    
    public void incrementS3Writes(String topic, long recordCount) {
        s3WritesCounter.increment();
        getTopicMetrics(topic).incrementS3Writes(recordCount);
    }
    
    public void incrementS3WriteFailures() {
        s3WriteFailuresCounter.increment();
    }
    
    public void incrementSchemaValidationFailures() {
        schemaValidationFailuresCounter.increment();
    }
    
    public void incrementDlqMessages() {
        dlqMessagesCounter.increment();
    }
    
    public void incrementCircuitBreakerOpen() {
        circuitBreakerOpenCounter.increment();
    }
    
    // Timer methods
    public Timer.Sample startRecordProcessingTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordProcessingTime(Timer.Sample sample) {
        sample.stop(recordProcessingTimer);
    }
    
    public Timer.Sample startS3WriteTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordS3WriteTime(Timer.Sample sample) {
        sample.stop(s3WriteTimer);
    }
    
    public Timer.Sample startSchemaValidationTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordSchemaValidationTime(Timer.Sample sample) {
        sample.stop(schemaValidationTimer);
    }
    
    // Gauge methods
    public void setActiveConnections(long count) {
        activeConnections.set(count);
    }
    
    public void incrementActiveConnections() {
        activeConnections.incrementAndGet();
    }
    
    public void decrementActiveConnections() {
        activeConnections.decrementAndGet();
    }
    
    public void setBufferedRecords(long count) {
        bufferedRecords.set(count);
    }
    
    public void setCurrentFileSize(long size) {
        currentFileSize.set(size);
    }
    
    // Per-topic metrics
    private TopicMetrics getTopicMetrics(String topic) {
        return topicMetrics.computeIfAbsent(topic, t -> new TopicMetrics(meterRegistry, t));
    }
    
    public TopicMetrics getTopicMetrics(String topic, boolean createIfNotExists) {
        if (createIfNotExists) {
            return getTopicMetrics(topic);
        }
        return topicMetrics.get(topic);
    }
    
    /**
     * Get all current metric values for monitoring dashboard
     */
    public ConnectorMetricsSnapshot getSnapshot() {
        return ConnectorMetricsSnapshot.builder()
            .recordsProcessed(recordsProcessedCounter.count())
            .recordsFailed(recordsFailedCounter.count())
            .s3Writes(s3WritesCounter.count())
            .s3WriteFailures(s3WriteFailuresCounter.count())
            .schemaValidationFailures(schemaValidationFailuresCounter.count())
            .dlqMessages(dlqMessagesCounter.count())
            .circuitBreakerOpens(circuitBreakerOpenCounter.count())
            .activeConnections(activeConnections.get())
            .bufferedRecords(bufferedRecords.get())
            .currentFileSize(currentFileSize.get())
            .avgRecordProcessingTime(recordProcessingTimer.mean(java.util.concurrent.TimeUnit.MILLISECONDS))
            .avgS3WriteTime(s3WriteTimer.mean(java.util.concurrent.TimeUnit.MILLISECONDS))
            .avgSchemaValidationTime(schemaValidationTimer.mean(java.util.concurrent.TimeUnit.MILLISECONDS))
            .build();
    }
}