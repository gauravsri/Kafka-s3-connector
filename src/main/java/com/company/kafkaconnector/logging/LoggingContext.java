package com.company.kafkaconnector.logging;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Logging context management for correlation IDs and structured logging
 * Following Utility Class pattern with comprehensive MDC management
 */
@UtilityClass
@Slf4j
public class LoggingContext {
    
    // MDC Keys
    public static final String CORRELATION_ID = "correlationId";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String OFFSET = "offset";
    public static final String OPERATION = "operation";
    public static final String COMPONENT = "component";
    public static final String BATCH_SIZE = "batchSize";
    public static final String FILE_PATH = "filePath";
    public static final String RECORD_COUNT = "recordCount";
    public static final String ERROR_TYPE = "errorType";
    public static final String RETRY_ATTEMPT = "retryAttempt";
    public static final String PROCESSING_TIME_MS = "processingTimeMs";
    
    /**
     * Generate and set a new correlation ID
     * @return The generated correlation ID
     */
    public static String generateCorrelationId() {
        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID, correlationId);
        return correlationId;
    }
    
    /**
     * Set correlation ID if not already present
     * @param correlationId The correlation ID to set
     */
    public static void setCorrelationId(String correlationId) {
        if (StringUtils.isNotBlank(correlationId)) {
            MDC.put(CORRELATION_ID, correlationId);
        }
    }
    
    /**
     * Get current correlation ID or generate one if not present
     * @return Current or new correlation ID
     */
    public static String getOrGenerateCorrelationId() {
        String correlationId = MDC.get(CORRELATION_ID);
        if (StringUtils.isBlank(correlationId)) {
            correlationId = generateCorrelationId();
        }
        return correlationId;
    }
    
    /**
     * Set Kafka topic context
     */
    public static void setTopicContext(String topic, Integer partition, Long offset) {
        if (StringUtils.isNotBlank(topic)) {
            MDC.put(TOPIC, topic);
        }
        if (partition != null) {
            MDC.put(PARTITION, partition.toString());
        }
        if (offset != null) {
            MDC.put(OFFSET, offset.toString());
        }
    }
    
    /**
     * Set operation context
     */
    public static void setOperationContext(String operation, String component) {
        if (StringUtils.isNotBlank(operation)) {
            MDC.put(OPERATION, operation);
        }
        if (StringUtils.isNotBlank(component)) {
            MDC.put(COMPONENT, component);
        }
    }
    
    /**
     * Set processing metrics context
     */
    public static void setProcessingContext(Integer batchSize, String filePath, Long recordCount) {
        if (batchSize != null) {
            MDC.put(BATCH_SIZE, batchSize.toString());
        }
        if (StringUtils.isNotBlank(filePath)) {
            MDC.put(FILE_PATH, filePath);
        }
        if (recordCount != null) {
            MDC.put(RECORD_COUNT, recordCount.toString());
        }
    }
    
    /**
     * Set error context
     */
    public static void setErrorContext(String errorType, Integer retryAttempt) {
        if (StringUtils.isNotBlank(errorType)) {
            MDC.put(ERROR_TYPE, errorType);
        }
        if (retryAttempt != null) {
            MDC.put(RETRY_ATTEMPT, retryAttempt.toString());
        }
    }
    
    /**
     * Set performance context
     */
    public static void setPerformanceContext(long processingTimeMs) {
        MDC.put(PROCESSING_TIME_MS, String.valueOf(processingTimeMs));
    }
    
    /**
     * Clear specific context keys
     */
    public static void clearTopicContext() {
        MDC.remove(TOPIC);
        MDC.remove(PARTITION);
        MDC.remove(OFFSET);
    }
    
    public static void clearProcessingContext() {
        MDC.remove(BATCH_SIZE);
        MDC.remove(FILE_PATH);
        MDC.remove(RECORD_COUNT);
        MDC.remove(PROCESSING_TIME_MS);
    }
    
    public static void clearErrorContext() {
        MDC.remove(ERROR_TYPE);
        MDC.remove(RETRY_ATTEMPT);
    }
    
    /**
     * Clear all context
     */
    public static void clearAll() {
        MDC.clear();
    }
    
    /**
     * Get a snapshot of current MDC context
     */
    public static Map<String, String> getContextSnapshot() {
        Map<String, String> contextMap = MDC.getCopyOfContextMap();
        return contextMap != null ? new HashMap<>(contextMap) : new HashMap<>();
    }
    
    /**
     * Execute operation with specific context
     */
    public static <T> T withContext(Map<String, String> context, java.util.function.Supplier<T> operation) {
        Map<String, String> originalContext = MDC.getCopyOfContextMap();
        try {
            // Set new context
            if (context != null) {
                context.forEach(MDC::put);
            }
            return operation.get();
        } finally {
            // Restore original context
            MDC.clear();
            if (originalContext != null) {
                originalContext.forEach(MDC::put);
            }
        }
    }
    
    /**
     * Execute operation with correlation ID
     */
    public static <T> T withCorrelationId(String correlationId, java.util.function.Supplier<T> operation) {
        String originalCorrelationId = MDC.get(CORRELATION_ID);
        try {
            setCorrelationId(correlationId);
            return operation.get();
        } finally {
            if (originalCorrelationId != null) {
                MDC.put(CORRELATION_ID, originalCorrelationId);
            } else {
                MDC.remove(CORRELATION_ID);
            }
        }
    }
    
    /**
     * Execute runnable with correlation ID
     */
    public static void withCorrelationId(String correlationId, Runnable operation) {
        withCorrelationId(correlationId, () -> {
            operation.run();
            return null;
        });
    }
}