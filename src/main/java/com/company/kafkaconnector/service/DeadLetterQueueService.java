package com.company.kafkaconnector.service;

import com.company.kafkaconnector.model.EventMessage;
import com.company.kafkaconnector.model.TopicConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class DeadLetterQueueService {
    
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueService.class);
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    // Track DLQ statistics
    private final Map<String, AtomicLong> dlqCounts = new ConcurrentHashMap<>();
    
    public DeadLetterQueueService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
    }
    
    public void sendToDeadLetterQueue(EventMessage originalMessage, TopicConfig topicConfig, 
                                     String errorReason, Exception exception) {
        try {
            String dlqTopic = buildDlqTopicName(topicConfig.getKafkaTopic());
            
            // Create DLQ message with metadata
            Map<String, Object> dlqMessage = createDlqMessage(originalMessage, errorReason, exception);
            
            String dlqMessageJson = objectMapper.writeValueAsString(dlqMessage);
            
            // Send to DLQ topic
            kafkaTemplate.send(dlqTopic, originalMessage.getKey(), dlqMessageJson)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        logger.info("Message sent to DLQ topic: {} for original topic: {}", 
                            dlqTopic, originalMessage.getTopicName());
                        incrementDlqCount(originalMessage.getTopicName());
                    } else {
                        logger.error("Failed to send message to DLQ topic: {} for original topic: {}. Error: {}", 
                            dlqTopic, originalMessage.getTopicName(), ex.getMessage());
                    }
                });
                
        } catch (Exception e) {
            logger.error("Error while sending message to DLQ for topic {}: {}", 
                originalMessage.getTopicName(), e.getMessage(), e);
        }
    }
    
    public void sendToDeadLetterQueue(String topicName, String messageKey, String messageValue, 
                                     String errorReason, Exception exception) {
        EventMessage eventMessage = new EventMessage();
        eventMessage.setTopicName(topicName);
        eventMessage.setKey(messageKey);
        eventMessage.setValue(messageValue);
        eventMessage.setTimestamp(LocalDateTime.now());
        
        // Create a basic topic config for DLQ
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setKafkaTopic(topicName);
        
        sendToDeadLetterQueue(eventMessage, topicConfig, errorReason, exception);
    }
    
    private String buildDlqTopicName(String originalTopic) {
        return originalTopic + "-dlq";
    }
    
    private Map<String, Object> createDlqMessage(EventMessage originalMessage, String errorReason, Exception exception) {
        Map<String, Object> dlqMessage = new HashMap<>();
        
        // Original message data
        dlqMessage.put("original_topic", originalMessage.getTopicName());
        dlqMessage.put("original_partition", originalMessage.getPartition());
        dlqMessage.put("original_offset", originalMessage.getOffset());
        dlqMessage.put("original_key", originalMessage.getKey());
        dlqMessage.put("original_value", originalMessage.getValue());
        dlqMessage.put("original_timestamp", originalMessage.getTimestamp());
        
        // Error information
        dlqMessage.put("error_reason", errorReason);
        dlqMessage.put("error_message", exception != null ? exception.getMessage() : "Unknown error");
        dlqMessage.put("error_class", exception != null ? exception.getClass().getSimpleName() : "Unknown");
        
        // DLQ metadata
        dlqMessage.put("dlq_timestamp", LocalDateTime.now());
        dlqMessage.put("dlq_version", "1.0");
        
        // Add stack trace for debugging (first few lines only)
        if (exception != null) {
            String stackTrace = getShortStackTrace(exception);
            dlqMessage.put("error_stack_trace", stackTrace);
        }
        
        return dlqMessage;
    }
    
    private String getShortStackTrace(Exception exception) {
        StringBuilder sb = new StringBuilder();
        StackTraceElement[] stackTrace = exception.getStackTrace();
        
        // Include first 5 stack trace elements
        for (int i = 0; i < Math.min(5, stackTrace.length); i++) {
            sb.append(stackTrace[i].toString()).append("\n");
        }
        
        return sb.toString();
    }
    
    private void incrementDlqCount(String topicName) {
        dlqCounts.computeIfAbsent(topicName, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    public Map<String, Long> getDlqStatistics() {
        Map<String, Long> stats = new HashMap<>();
        dlqCounts.forEach((topic, count) -> stats.put(topic, count.get()));
        return stats;
    }
    
    public long getDlqCount(String topicName) {
        AtomicLong count = dlqCounts.get(topicName);
        return count != null ? count.get() : 0;
    }
    
    /**
     * Process messages from DLQ topic for reprocessing
     * This would typically be triggered by an admin endpoint or scheduled job
     */
    public void reprocessDlqMessages(String dlqTopic, int maxMessages) {
        logger.info("Starting DLQ reprocessing for topic: {} (max messages: {})", dlqTopic, maxMessages);
        
        // Implementation for DLQ message reprocessing would go here
        // This would involve reading from the DLQ topic, validating messages,
        // and attempting to reprocess them through the main pipeline
        // This would involve:
        // 1. Consuming messages from DLQ topic
        // 2. Attempting to reprocess them through the normal pipeline
        // 3. Tracking success/failure rates
        // 4. Moving messages to a different topic if they continue to fail
        
        logger.warn("DLQ reprocessing not yet implemented for topic: {}", dlqTopic);
    }
}