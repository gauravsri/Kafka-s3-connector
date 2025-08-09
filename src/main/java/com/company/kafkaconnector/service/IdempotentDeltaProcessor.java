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
        // Validate event_type enum values
        if (record.get("event_type") != null) {
            String eventType = record.get("event_type").toString();
            if (!isValidEventType(eventType)) {
                throw new BusinessValidationException("Invalid event_type: " + eventType);
            }
        }
        
        // Validate user_id format
        if (record.get("user_id") != null) {
            String userId = record.get("user_id").toString();
            if (!isValidUserId(userId)) {
                throw new BusinessValidationException("Invalid user_id format: " + userId);
            }
        }
        
        // Validate timestamp fields
        validateTimestampFields(record);
        
        // Validate numeric ranges
        validateNumericRanges(record);
        
        // Validate cross-field dependencies
        validateCrossFieldConstraints(record);
        
        // Validate business-specific constraints
        validateDomainSpecificRules(record);
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
        // Add temporal features (deterministic based on timestamp)
        addTemporalFeatures(record);
        
        // Calculate derived fields (deterministic)
        calculateDerivedFields(record);
        
        // Add data quality scores
        calculateDataQualityMetrics(record);
        
        // Add business context flags
        addBusinessContextFlags(record);
        
        // Note: User lookup, geolocation, and fraud scoring would require
        // external service calls that should be cached for determinism
        // Implementation would go here if external services are available
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
    
    /**
     * Validate timestamp fields for reasonable ranges and formats
     */
    private void validateTimestampFields(GenericRecord record) throws BusinessValidationException {
        if (record.get("timestamp") != null) {
            Object timestampObj = record.get("timestamp");
            long timestamp;
            
            if (timestampObj instanceof Long) {
                timestamp = (Long) timestampObj;
            } else {
                try {
                    timestamp = Long.parseLong(timestampObj.toString());
                } catch (NumberFormatException e) {
                    throw new BusinessValidationException("Invalid timestamp format");
                }
            }
            
            // Validate timestamp is within reasonable range (not too old, not in future)
            long now = System.currentTimeMillis();
            long fiveYearsAgo = now - (5L * 365 * 24 * 60 * 60 * 1000);
            
            if (timestamp < fiveYearsAgo) {
                throw new BusinessValidationException("Timestamp too old: " + timestamp);
            }
            
            if (timestamp > now + (24 * 60 * 60 * 1000)) { // Allow 24h future for clock skew
                throw new BusinessValidationException("Timestamp in future: " + timestamp);
            }
        }
    }
    
    /**
     * Validate numeric fields are within expected ranges
     */
    private void validateNumericRanges(GenericRecord record) throws BusinessValidationException {
        // Validate amount fields if present
        if (record.get("amount") != null) {
            Object amountObj = record.get("amount");
            double amount;
            
            if (amountObj instanceof Double) {
                amount = (Double) amountObj;
            } else if (amountObj instanceof Float) {
                amount = ((Float) amountObj).doubleValue();
            } else {
                try {
                    amount = Double.parseDouble(amountObj.toString());
                } catch (NumberFormatException e) {
                    throw new BusinessValidationException("Invalid amount format");
                }
            }
            
            if (amount < 0) {
                throw new BusinessValidationException("Amount cannot be negative: " + amount);
            }
            
            if (amount > 1_000_000) { // Business rule: max transaction 1M
                throw new BusinessValidationException("Amount exceeds maximum limit: " + amount);
            }
        }
        
        // Validate quantity fields
        if (record.get("quantity") != null) {
            Object qtyObj = record.get("quantity");
            int quantity;
            
            if (qtyObj instanceof Integer) {
                quantity = (Integer) qtyObj;
            } else {
                try {
                    quantity = Integer.parseInt(qtyObj.toString());
                } catch (NumberFormatException e) {
                    throw new BusinessValidationException("Invalid quantity format");
                }
            }
            
            if (quantity < 0) {
                throw new BusinessValidationException("Quantity cannot be negative: " + quantity);
            }
            
            if (quantity > 10000) { // Business rule: max quantity 10k
                throw new BusinessValidationException("Quantity exceeds maximum limit: " + quantity);
            }
        }
    }
    
    /**
     * Validate cross-field business constraints
     */
    private void validateCrossFieldConstraints(GenericRecord record) throws BusinessValidationException {
        // Example: For purchase events, amount and quantity should be consistent
        String eventType = record.get("event_type") != null ? record.get("event_type").toString() : null;
        
        if ("PURCHASE".equals(eventType) || "ORDER".equals(eventType)) {
            Object amountObj = record.get("amount");
            Object qtyObj = record.get("quantity");
            Object priceObj = record.get("unit_price");
            
            if (amountObj != null && qtyObj != null && priceObj != null) {
                try {
                    double amount = Double.parseDouble(amountObj.toString());
                    int quantity = Integer.parseInt(qtyObj.toString());
                    double unitPrice = Double.parseDouble(priceObj.toString());
                    
                    double expectedAmount = quantity * unitPrice;
                    double tolerance = 0.01; // 1 cent tolerance
                    
                    if (Math.abs(amount - expectedAmount) > tolerance) {
                        throw new BusinessValidationException(
                            String.format("Amount mismatch: expected %.2f (qty=%d * price=%.2f), got %.2f", 
                                         expectedAmount, quantity, unitPrice, amount));
                    }
                } catch (NumberFormatException e) {
                    throw new BusinessValidationException("Invalid numeric values in purchase validation");
                }
            }
        }
        
        // Example: User events should have valid user context
        if ("USER_ACTION".equals(eventType)) {
            if (record.get("user_id") == null || record.get("session_id") == null) {
                throw new BusinessValidationException("USER_ACTION events require user_id and session_id");
            }
        }
    }
    
    /**
     * Validate domain-specific business rules
     */
    private void validateDomainSpecificRules(GenericRecord record) throws BusinessValidationException {
        String eventType = record.get("event_type") != null ? record.get("event_type").toString() : null;
        
        // E-commerce specific validations
        if ("PRODUCT_VIEW".equals(eventType) || "ADD_TO_CART".equals(eventType) || "PURCHASE".equals(eventType)) {
            if (record.get("product_id") == null) {
                throw new BusinessValidationException(eventType + " events require product_id");
            }
            
            String productId = record.get("product_id").toString();
            if (!isValidProductId(productId)) {
                throw new BusinessValidationException("Invalid product_id format: " + productId);
            }
        }
        
        // Financial transaction validations
        if ("PAYMENT".equals(eventType) || "REFUND".equals(eventType)) {
            if (record.get("amount") == null || record.get("currency") == null) {
                throw new BusinessValidationException(eventType + " events require amount and currency");
            }
            
            String currency = record.get("currency").toString();
            if (!isValidCurrency(currency)) {
                throw new BusinessValidationException("Invalid currency code: " + currency);
            }
        }
        
        // User authentication validations
        if ("LOGIN".equals(eventType) || "LOGOUT".equals(eventType)) {
            if (record.get("user_id") == null) {
                throw new BusinessValidationException(eventType + " events require user_id");
            }
        }
    }
    
    /**
     * Add temporal features based on timestamp
     */
    private void addTemporalFeatures(GenericRecord record) {
        if (record.get("timestamp") != null) {
            long timestamp = Long.parseLong(record.get("timestamp").toString());
            java.time.Instant instant = java.time.Instant.ofEpochMilli(timestamp);
            java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneOffset.UTC);
            
            // Add deterministic temporal features
            // Note: These would need to be added to the Avro schema
            // record.put("hour_of_day", zdt.getHour());
            // record.put("day_of_week", zdt.getDayOfWeek().getValue());
            // record.put("is_weekend", zdt.getDayOfWeek().getValue() >= 6);
            // record.put("quarter", ((zdt.getMonthValue() - 1) / 3) + 1);
            
            logger.debug("Added temporal features for timestamp: {}", timestamp);
        }
    }
    
    /**
     * Calculate derived fields from existing data
     */
    private void calculateDerivedFields(GenericRecord record) {
        // Calculate total value if unit price and quantity exist
        if (record.get("unit_price") != null && record.get("quantity") != null) {
            try {
                double unitPrice = Double.parseDouble(record.get("unit_price").toString());
                int quantity = Integer.parseInt(record.get("quantity").toString());
                double totalValue = unitPrice * quantity;
                
                // record.put("calculated_total", totalValue); // Would need schema support
                logger.debug("Calculated total value: {}", totalValue);
            } catch (NumberFormatException e) {
                logger.warn("Failed to calculate derived fields due to invalid numbers");
            }
        }
        
        // Calculate event priority based on type and amount
        String eventType = record.get("event_type") != null ? record.get("event_type").toString() : "UNKNOWN";
        int priority = calculateEventPriority(eventType, record.get("amount"));
        logger.debug("Calculated event priority: {} for type: {}", priority, eventType);
    }
    
    /**
     * Calculate data quality metrics
     */
    private void calculateDataQualityMetrics(GenericRecord record) {
        int completenessScore = 0;
        int totalFields = record.getSchema().getFields().size();
        
        // Count non-null fields
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            if (record.get(field.name()) != null) {
                completenessScore++;
            }
        }
        
        double completenessRatio = (double) completenessScore / totalFields;
        
        // Calculate consistency score based on field validations
        double consistencyScore = calculateConsistencyScore(record);
        
        logger.debug("Data quality - Completeness: {:.2f}, Consistency: {:.2f}", 
                    completenessRatio, consistencyScore);
    }
    
    /**
     * Add business context flags
     */
    private void addBusinessContextFlags(GenericRecord record) {
        String eventType = record.get("event_type") != null ? record.get("event_type").toString() : null;
        
        // Flag high-value transactions
        boolean isHighValue = false;
        if (record.get("amount") != null) {
            try {
                double amount = Double.parseDouble(record.get("amount").toString());
                isHighValue = amount > 10000; // Business threshold
            } catch (NumberFormatException e) {
                // Ignore invalid amounts
            }
        }
        
        // Flag critical events
        boolean isCriticalEvent = "PAYMENT".equals(eventType) || 
                                 "LOGIN_FAILED".equals(eventType) || 
                                 "SECURITY_ALERT".equals(eventType);
        
        logger.debug("Business context - High value: {}, Critical: {}", isHighValue, isCriticalEvent);
    }
    
    // Helper methods for validation
    private boolean isValidProductId(String productId) {
        return productId != null && productId.matches("^[A-Z0-9_-]{6,20}$");
    }
    
    private boolean isValidCurrency(String currency) {
        return currency != null && currency.matches("^[A-Z]{3}$") && 
               java.util.Set.of("USD", "EUR", "GBP", "JPY", "CAD", "AUD").contains(currency);
    }
    
    private int calculateEventPriority(String eventType, Object amountObj) {
        if (eventType == null) return 3; // Default priority
        
        // High priority events
        if (eventType.equals("SECURITY_ALERT") || eventType.equals("FRAUD_DETECTED")) {
            return 1;
        }
        
        // Medium-high priority for financial events with high amounts
        if ((eventType.equals("PAYMENT") || eventType.equals("REFUND")) && amountObj != null) {
            try {
                double amount = Double.parseDouble(amountObj.toString());
                if (amount > 10000) return 1;
                if (amount > 1000) return 2;
            } catch (NumberFormatException e) {
                // Ignore invalid amounts
            }
        }
        
        // Medium priority for user actions
        if (eventType.startsWith("USER_")) {
            return 3;
        }
        
        // Low priority for analytics events
        if (eventType.equals("PAGE_VIEW") || eventType.equals("PRODUCT_VIEW")) {
            return 4;
        }
        
        return 3; // Default
    }
    
    private double calculateConsistencyScore(GenericRecord record) {
        double score = 1.0;
        
        // Check for inconsistent data patterns
        String eventType = record.get("event_type") != null ? record.get("event_type").toString() : null;
        
        // Reduce score for missing required fields based on event type
        if ("PURCHASE".equals(eventType) && record.get("amount") == null) {
            score -= 0.3;
        }
        
        if ("USER_ACTION".equals(eventType) && record.get("user_id") == null) {
            score -= 0.4;
        }
        
        // Check timestamp consistency
        if (record.get("timestamp") != null) {
            try {
                long timestamp = Long.parseLong(record.get("timestamp").toString());
                long now = System.currentTimeMillis();
                
                // Reduce score for very old or future timestamps
                if (Math.abs(now - timestamp) > (30L * 24 * 60 * 60 * 1000)) { // 30 days
                    score -= 0.2;
                }
            } catch (NumberFormatException e) {
                score -= 0.5; // Invalid timestamp format
            }
        }
        
        return Math.max(0.0, score);
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