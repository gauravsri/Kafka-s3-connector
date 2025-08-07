package com.company.kafkaconnector.connector;

import com.company.kafkaconnector.factory.WriterFactory;
import com.company.kafkaconnector.logging.LoggingContext;
import com.company.kafkaconnector.service.RecordProcessor;
import com.company.kafkaconnector.strategy.PartitionStrategy;
import com.company.kafkaconnector.strategy.PartitionStrategyFactory;
import com.company.kafkaconnector.writer.S3Writer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * S3 Delta Lake Sink Task implementing proper separation of concerns
 * Uses Strategy Pattern for partitioning, Factory Pattern for writers
 */
@Slf4j
public class S3DeltaSinkTask extends SinkTask {
    
    private Map<String, String> config;
    private RecordProcessor recordProcessor;
    private PartitionStrategy partitionStrategy;
    private WriterFactory writerFactory;
    private final Map<String, S3Writer> writers = new ConcurrentHashMap<>();
    private final Map<String, List<SinkRecord>> recordBuffers = new ConcurrentHashMap<>();
    
    private int flushSize;
    private long rotateIntervalMs;
    private long lastRotateTime;
    
    @Override
    public String version() {
        return S3DeltaSinkConnector.VERSION;
    }
    
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting S3 Delta Sink Task");
        
        this.config = props;
        initializeComponents();
        initializeConfiguration();
        
        log.info("S3 Delta Sink Task started successfully");
    }
    
    @Override
    public void put(Collection<SinkRecord> records) {
        if (CollectionUtils.isEmpty(records)) {
            return;
        }
        
        String correlationId = LoggingContext.generateCorrelationId();
        LoggingContext.setOperationContext("PUT_RECORDS", "S3DeltaSinkTask");
        LoggingContext.setProcessingContext(records.size(), null, (long) records.size());
        
        try {
            log.info("Processing {} records with correlationId {}", records.size(), correlationId);
            
            // Process and buffer records by topic/partition
            for (SinkRecord record : records) {
                LoggingContext.setTopicContext(record.topic(), record.kafkaPartition(), record.kafkaOffset());
                processRecord(record);
            }
            
            // Check if we need to flush based on buffer size or time
            checkAndFlushBuffers();
            
        } catch (Exception e) {
            LoggingContext.setErrorContext(e.getClass().getSimpleName(), null);
            log.error("Error processing records", e);
            throw new RuntimeException("Failed to process sink records", e);
        } finally {
            LoggingContext.clearAll();
        }
    }
    
    @Override
    public void flush(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> currentOffsets) {
        String correlationId = LoggingContext.generateCorrelationId();
        LoggingContext.setOperationContext("FLUSH_BUFFERS", "S3DeltaSinkTask");
        
        try {
            log.info("Flushing all buffers for {} topic partitions with correlationId {}", 
                    currentOffsets.size(), correlationId);
            flushAllBuffers();
        } catch (Exception e) {
            LoggingContext.setErrorContext(e.getClass().getSimpleName(), null);
            log.error("Error during flush operation", e);
            throw new RuntimeException("Failed to flush records", e);
        } finally {
            LoggingContext.clearAll();
        }
    }
    
    @Override
    public void stop() {
        log.info("Stopping S3 Delta Sink Task");
        
        try {
            // Flush any remaining records
            flushAllBuffers();
            
            // Close all writers
            writers.values().forEach(writer -> {
                try {
                    writer.close();
                } catch (Exception e) {
                    log.warn("Error closing writer", e);
                }
            });
            
            writers.clear();
            recordBuffers.clear();
            
        } catch (Exception e) {
            log.error("Error stopping sink task", e);
        }
        
        log.info("S3 Delta Sink Task stopped");
    }
    
    /**
     * Initialize components using Dependency Injection pattern
     * Following Inversion of Control principle
     */
    private void initializeComponents() {
        this.recordProcessor = new RecordProcessor();
        this.partitionStrategy = PartitionStrategyFactory.createStrategy(config);
        this.writerFactory = new WriterFactory(config);
    }
    
    /**
     * Initialize configuration settings
     * Following Single Responsibility Principle
     */
    private void initializeConfiguration() {
        this.flushSize = Integer.parseInt(
            config.getOrDefault(S3DeltaSinkConnector.FLUSH_SIZE_CONFIG, "1000"));
        this.rotateIntervalMs = Long.parseLong(
            config.getOrDefault(S3DeltaSinkConnector.ROTATE_INTERVAL_MS_CONFIG, "60000"));
        this.lastRotateTime = System.currentTimeMillis();
    }
    
    /**
     * Process a single record using Strategy Pattern
     */
    private void processRecord(SinkRecord record) {
        try {
            // Transform record using processor
            SinkRecord processedRecord = recordProcessor.process(record);
            
            // Determine partition key using strategy
            String partitionKey = partitionStrategy.getPartitionKey(processedRecord);
            
            // Buffer the record
            recordBuffers.computeIfAbsent(partitionKey, k -> new ArrayList<>())
                         .add(processedRecord);
            
            log.trace("Buffered record for partition key: {}", partitionKey);
            
        } catch (Exception e) {
            log.error("Error processing record from topic {} partition {}", 
                     record.topic(), record.kafkaPartition(), e);
            throw e;
        }
    }
    
    /**
     * Check if buffers need to be flushed based on size or time
     * Following Open/Closed Principle - extensible flush conditions
     */
    private void checkAndFlushBuffers() {
        boolean shouldFlush = false;
        
        // Check buffer size
        for (List<SinkRecord> buffer : recordBuffers.values()) {
            if (buffer.size() >= flushSize) {
                shouldFlush = true;
                break;
            }
        }
        
        // Check time-based rotation
        if (!shouldFlush && System.currentTimeMillis() - lastRotateTime >= rotateIntervalMs) {
            shouldFlush = true;
        }
        
        if (shouldFlush) {
            flushAllBuffers();
        }
    }
    
    /**
     * Flush all buffered records to S3
     * Using Factory Pattern for writer creation
     */
    private void flushAllBuffers() {
        for (Map.Entry<String, List<SinkRecord>> entry : recordBuffers.entrySet()) {
            String partitionKey = entry.getKey();
            List<SinkRecord> records = entry.getValue();
            
            if (CollectionUtils.isEmpty(records)) {
                continue;
            }
            
            try {
                // Get or create writer for this partition
                S3Writer writer = writers.computeIfAbsent(partitionKey, 
                    k -> writerFactory.createWriter(k));
                
                // Write records
                writer.write(records);
                
                // Clear buffer after successful write
                records.clear();
                
                log.debug("Flushed {} records for partition key: {}", 
                         records.size(), partitionKey);
                
            } catch (Exception e) {
                log.error("Error flushing records for partition key: {}", partitionKey, e);
                throw new RuntimeException("Failed to flush records for partition: " + partitionKey, e);
            }
        }
        
        lastRotateTime = System.currentTimeMillis();
        log.info("Completed flushing all buffers at {}", new Date(lastRotateTime));
    }
}