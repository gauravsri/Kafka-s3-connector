package com.company.kafkaconnector.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-topic metrics implementation
 * Following Single Responsibility Principle for topic-specific metrics
 */
@Slf4j
public class TopicMetrics {
    
    private final String topicName;
    private final MeterRegistry meterRegistry;
    
    // Per-topic counters
    private final Counter recordsProcessedCounter;
    private final Counter recordsFailedCounter;
    private final Counter s3WritesCounter;
    private final Counter bytesWrittenCounter;
    
    // Per-topic gauges
    private final AtomicLong bufferedRecordsCount = new AtomicLong(0);
    private final AtomicLong lastWriteTimestamp = new AtomicLong(0);
    
    public TopicMetrics(MeterRegistry meterRegistry, String topicName) {
        this.meterRegistry = meterRegistry;
        this.topicName = topicName;
        
        Tags topicTags = Tags.of("topic", topicName);
        
        // Initialize per-topic counters
        this.recordsProcessedCounter = Counter.builder("kafka_connector_topic_records_processed_total")
            .description("Total records processed per topic")
            .tags(topicTags)
            .register(meterRegistry);
            
        this.recordsFailedCounter = Counter.builder("kafka_connector_topic_records_failed_total")
            .description("Total failed records per topic")
            .tags(topicTags)
            .register(meterRegistry);
            
        this.s3WritesCounter = Counter.builder("kafka_connector_topic_s3_writes_total")
            .description("Total S3 writes per topic")
            .tags(topicTags)
            .register(meterRegistry);
            
        this.bytesWrittenCounter = Counter.builder("kafka_connector_topic_bytes_written_total")
            .description("Total bytes written to S3 per topic")
            .tags(topicTags)
            .register(meterRegistry);
        
        // Initialize per-topic gauges
        Gauge.builder("kafka_connector_topic_buffered_records", this, topicMetrics -> topicMetrics.bufferedRecordsCount.get())
            .description("Buffered records per topic")
            .tags(topicTags)
            .register(meterRegistry);
            
        Gauge.builder("kafka_connector_topic_last_write_timestamp", this, topicMetrics -> topicMetrics.lastWriteTimestamp.get())
            .description("Last write timestamp per topic (epoch millis)")
            .tags(topicTags)
            .register(meterRegistry);
    }
    
    public void incrementRecordsProcessed() {
        recordsProcessedCounter.increment();
    }
    
    public void incrementRecordsProcessed(long count) {
        recordsProcessedCounter.increment(count);
    }
    
    public void incrementRecordsFailed() {
        recordsFailedCounter.increment();
    }
    
    public void incrementRecordsFailed(long count) {
        recordsFailedCounter.increment(count);
    }
    
    public void incrementS3Writes(long recordCount) {
        s3WritesCounter.increment();
        // Also increment the total records processed for this write
        incrementRecordsProcessed(recordCount);
    }
    
    public void addBytesWritten(long bytes) {
        bytesWrittenCounter.increment(bytes);
    }
    
    public void setBufferedRecordsCount(long count) {
        bufferedRecordsCount.set(count);
    }
    
    public void incrementBufferedRecordsCount() {
        bufferedRecordsCount.incrementAndGet();
    }
    
    public void decrementBufferedRecordsCount() {
        long current = bufferedRecordsCount.get();
        if (current > 0) {
            bufferedRecordsCount.decrementAndGet();
        }
    }
    
    public void updateLastWriteTimestamp() {
        lastWriteTimestamp.set(System.currentTimeMillis());
    }
    
    public String getTopicName() {
        return topicName;
    }
    
    public double getRecordsProcessedCount() {
        return recordsProcessedCounter.count();
    }
    
    public double getRecordsFailedCount() {
        return recordsFailedCounter.count();
    }
    
    public double getS3WritesCount() {
        return s3WritesCounter.count();
    }
    
    public double getBytesWrittenCount() {
        return bytesWrittenCounter.count();
    }
    
    public long getBufferedRecordsCount() {
        return bufferedRecordsCount.get();
    }
    
    public long getLastWriteTimestamp() {
        return lastWriteTimestamp.get();
    }
}