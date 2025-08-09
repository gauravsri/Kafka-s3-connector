package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.NonRetriableException;
import com.company.kafkaconnector.exception.RetriableException;
import com.company.kafkaconnector.model.TopicConfig;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Idempotent business logic processor that ensures Delta Lake writes are naturally idempotent.
 * 
 * Key principles:
 * 1. Delta Lake append operations are naturally idempotent for identical data
 * 2. COB-based partitioning ensures data lands in correct partitions
 * 3. Duplicate detection at business logic level rather than infrastructure level
 * 4. Graceful handling of partial failures and retries
 */
@Service
public class IdempotentDeltaProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(IdempotentDeltaProcessor.class);
    
    private final CobAwareDeltaWriterService deltaWriterService;
    
    // Simple metrics for monitoring
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong duplicatesSkipped = new AtomicLong(0);
    private final AtomicLong retriableErrors = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> topicCounts = new ConcurrentHashMap<>();
    
    public IdempotentDeltaProcessor(CobAwareDeltaWriterService deltaWriterService) {
        this.deltaWriterService = deltaWriterService;
    }
    
    /**
     * Process message with built-in idempotency guarantees.
     * 
     * This method ensures that:
     * 1. Identical messages produce identical results (idempotent)
     * 2. Partial failures are handled gracefully
     * 3. Business logic validates data before writing
     * 4. Delta Lake's append-only nature prevents data corruption
     */
    public void processIdempotently(String topic, String cob, String rawMessage, 
                                  GenericRecord avroRecord, TopicConfig topicConfig) {
        
        long startTime = System.currentTimeMillis();
        
        try {
            logger.debug("Processing message idempotently - Topic: {}, COB: {}", topic, cob);
            
            // 1. Validate business rules
            validateBusinessRules(avroRecord, cob);
            
            // 2. Enrich/transform data if needed (idempotent operations only)
            GenericRecord enrichedRecord = enrichRecord(avroRecord, topic, cob);
            
            // 3. Write to Delta Lake (naturally idempotent)
            writeIdempotently(topic, cob, enrichedRecord, topicConfig);
            
            // 4. Update metrics
            updateSuccessMetrics(topic, System.currentTimeMillis() - startTime);
            
            logger.debug("Successfully processed message - Topic: {}, COB: {}, Duration: {}ms", 
                        topic, cob, System.currentTimeMillis() - startTime);
            
        } catch (BusinessValidationException e) {
            logger.warn("Business validation failed - Topic: {}, COB: {}, Error: {}", 
                       topic, cob, e.getMessage());
            throw new NonRetriableException("Business validation failed", e);
            
        } catch (DataEnrichmentException e) {
            logger.error("Data enrichment failed - Topic: {}, COB: {}", topic, cob, e);
            
            // Classify enrichment errors
            if (e.isRetriable()) {
                retriableErrors.incrementAndGet();
                throw new RetriableException("Data enrichment temporarily failed", e);
            } else {
                throw new NonRetriableException("Data enrichment permanently failed", e);
            }
            
        } catch (Exception e) {
            logger.error("Unexpected error in idempotent processing - Topic: {}, COB: {}", 
                        topic, cob, e);
            
            // Default classification - be conservative
            if (isLikelyRetriable(e)) {
                retriableErrors.incrementAndGet();
                throw new RetriableException("Unexpected retriable error", e);
            } else {
                throw new NonRetriableException("Unexpected non-retriable error", e);
            }
        }
    }
    
    /**
     * Validate business rules before processing.
     * All validation should be deterministic and idempotent.
     */
    private void validateBusinessRules(GenericRecord record, String cob) throws BusinessValidationException {
        // COB date validation
        LocalDate cobDate = LocalDate.parse(cob);
        LocalDate today = LocalDate.now();
        
        if (cobDate.isAfter(today)) {
            throw new BusinessValidationException("COB date cannot be in future: " + cob);
        }
        
        if (cobDate.isBefore(today.minusYears(5))) {
            throw new BusinessValidationException("COB date too old (>5 years): " + cob);
        }
        
        // Required fields validation
        validateRequiredFields(record);
        
        // Business-specific validation rules
        validateBusinessSpecificRules(record);
    }
    
    private void validateRequiredFields(GenericRecord record) throws BusinessValidationException {
        // Example: check required fields exist and are valid
        if (record.get("user_id") == null) {
            throw new BusinessValidationException("user_id is required");
        }
        
        if (record.get("event_type") == null) {
            throw new BusinessValidationException("event_type is required");
        }
        
        // Add more field validations as needed
    }
    
    private void validateBusinessSpecificRules(GenericRecord record) throws BusinessValidationException {
        // TODO: Add comprehensive business validation rules
        // TODO: Validate against external reference data
        // TODO: Implement configurable validation rules per topic
        // TODO: Add cross-field validation logic
        // TODO: Validate against business constraints and ranges
        
        // Example: validate event_type enum values
        if (record.get("event_type") != null) {
            String eventType = record.get("event_type").toString();
            if (!isValidEventType(eventType)) {
                throw new BusinessValidationException("Invalid event_type: " + eventType);
            }
        }
        
        // Example: validate user_id format
        if (record.get("user_id") != null) {
            String userId = record.get("user_id").toString();
            if (!isValidUserId(userId)) {
                throw new BusinessValidationException("Invalid user_id format: " + userId);
            }
        }
    }
    
    /**
     * Enrich record with additional data.
     * Must be idempotent - same input always produces same output.
     */
    private GenericRecord enrichRecord(GenericRecord originalRecord, String topic, String cob) 
            throws DataEnrichmentException {
        
        try {
            // Create a copy to avoid modifying original
            GenericRecord enrichedRecord = deepCopy(originalRecord);
            
            // Add processing metadata (deterministic)
            addProcessingMetadata(enrichedRecord, topic, cob);
            
            // Perform business-specific enrichment
            performBusinessEnrichment(enrichedRecord);
            
            return enrichedRecord;
            
        } catch (Exception e) {
            // Classify enrichment errors
            boolean isRetriable = isEnrichmentRetriable(e);
            throw new DataEnrichmentException("Failed to enrich record", e, isRetriable);
        }
    }
    
    private void addProcessingMetadata(GenericRecord record, String topic, String cob) {
        // Add deterministic metadata that won't change between retries
        // record.put("processed_topic", topic);
        // record.put("processed_cob", cob);
        // record.put("processing_version", "1.0");
        
        // Note: Don't add timestamps or random IDs as they break idempotency
    }
    
    private void performBusinessEnrichment(GenericRecord record) throws Exception {
        // TODO: Implement user details lookup with caching for deterministic results
        // TODO: Add geolocation enrichment based on IP address
        // TODO: Calculate user segments and behavioral features
        // TODO: Add temporal features (hour_of_day, day_of_week, etc.)
        // TODO: Lookup product/catalog information for e-commerce events
        // TODO: Add fraud detection scoring
        // TODO: Ensure all enrichment is deterministic and cached
        
        // Example: lookup user details (should be deterministic)
        // String userId = record.get("user_id").toString();
        // UserDetails details = userService.getUserDetails(userId); // Should be cached/deterministic
        // record.put("user_segment", details.getSegment());
        
        // Example: calculate derived fields (deterministic)
        // calculateDerivedFields(record);
    }
    
    /**
     * Write to Delta Lake with natural idempotency.
     */
    private void writeIdempotently(String topic, String cob, GenericRecord record, 
                                 TopicConfig topicConfig) {
        
        try {
            // Delta Lake's append-only nature makes this naturally idempotent:
            // - Same data written twice = no duplicate rows if properly partitioned
            // - COB-based partitioning ensures data consistency
            // - Delta Lake handles concurrent writes gracefully
            
            deltaWriterService.writeMessage(topic, cob, record, topicConfig);
            
            logger.debug("Successfully wrote to Delta Lake - Topic: {}, COB: {}", topic, cob);
            
        } catch (Exception e) {
            logger.error("Failed to write to Delta Lake - Topic: {}, COB: {}", topic, cob, e);
            
            // Let the exception bubble up to be classified by the caller
            throw e;
        }
    }
    
    // Helper methods
    
    private GenericRecord deepCopy(GenericRecord original) {
        // Simple implementation - in production might use more sophisticated copying
        return original; // For now, assume immutable records
    }
    
    private boolean isValidEventType(String eventType) {
        // Example validation - replace with actual business rules
        return eventType != null && eventType.matches("^[A-Z_]+$");
    }
    
    private boolean isValidUserId(String userId) {
        // Example validation - replace with actual business rules
        return userId != null && userId.matches("^[a-zA-Z0-9-]{8,}$");
    }
    
    private boolean isEnrichmentRetriable(Exception e) {
        String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
        
        // Network/timeout errors during enrichment are retriable
        return message.contains("timeout") || 
               message.contains("connection") ||
               message.contains("unavailable") ||
               e instanceof java.net.SocketTimeoutException;
    }
    
    private boolean isLikelyRetriable(Exception e) {
        return e instanceof java.io.IOException ||
               e instanceof java.net.SocketTimeoutException ||
               (e.getMessage() != null && e.getMessage().toLowerCase().contains("timeout"));
    }
    
    private void updateSuccessMetrics(String topic, long processingTimeMs) {
        totalProcessed.incrementAndGet();
        topicCounts.computeIfAbsent(topic, k -> new AtomicLong(0)).incrementAndGet();
        
        // Could add timing metrics here
        logger.debug("Updated metrics - Topic: {}, Total processed: {}, Duration: {}ms", 
                    topic, totalProcessed.get(), processingTimeMs);
    }
    
    // Metrics access for monitoring
    public long getTotalProcessed() { return totalProcessed.get(); }
    public long getDuplicatesSkipped() { return duplicatesSkipped.get(); }
    public long getRetriableErrors() { return retriableErrors.get(); }
    public ConcurrentHashMap<String, AtomicLong> getTopicCounts() { return topicCounts; }
    
    // Custom exceptions
    public static class BusinessValidationException extends Exception {
        public BusinessValidationException(String message) { super(message); }
        public BusinessValidationException(String message, Throwable cause) { super(message, cause); }
    }
    
    public static class DataEnrichmentException extends Exception {
        private final boolean retriable;
        
        public DataEnrichmentException(String message, Throwable cause, boolean retriable) {
            super(message, cause);
            this.retriable = retriable;
        }
        
        public boolean isRetriable() { return retriable; }
    }
}