package com.company.kafkaconnector.service;

import com.company.kafkaconnector.config.ConnectorConfiguration;
import com.company.kafkaconnector.exception.TopicConfigurationException;
import com.company.kafkaconnector.model.EventMessage;
import com.company.kafkaconnector.model.ProcessingResult;
import com.company.kafkaconnector.model.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TopicRouterService {
    
    private static final Logger logger = LoggerFactory.getLogger(TopicRouterService.class);
    
    private final ConnectorConfiguration connectorConfiguration;
    private final SchemaValidationService schemaValidationService;
    private final MessageTransformationService messageTransformationService;
    private final DeltaWriterService deltaWriterService;
    private final RetryService retryService;
    private final CircuitBreakerService circuitBreakerService;
    private final DeadLetterQueueService deadLetterQueueService;
    
    // Track processing statistics per topic
    private final Map<String, ProcessingResult> topicStats = new ConcurrentHashMap<>();

    @Autowired
    public TopicRouterService(
            ConnectorConfiguration connectorConfiguration,
            SchemaValidationService schemaValidationService,
            MessageTransformationService messageTransformationService,
            DeltaWriterService deltaWriterService,
            RetryService retryService,
            CircuitBreakerService circuitBreakerService,
            DeadLetterQueueService deadLetterQueueService) {
        this.connectorConfiguration = connectorConfiguration;
        this.schemaValidationService = schemaValidationService;
        this.messageTransformationService = messageTransformationService;
        this.deltaWriterService = deltaWriterService;
        this.retryService = retryService;
        this.circuitBreakerService = circuitBreakerService;
        this.deadLetterQueueService = deadLetterQueueService;
    }

    public ProcessingResult processMessage(ConsumerRecord<String, String> record) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        
        try {
            String topicName = record.topic();
            logger.debug("Processing message from topic: {}", topicName);
            
            // Find topic configuration
            TopicConfig topicConfig = findTopicConfig(topicName);
            if (topicConfig == null) {
                throw new TopicConfigurationException(
                    "No configuration found for topic: " + topicName, 
                    topicName, 
                    "topic-mapping"
                );
            }

            // Create event message
            EventMessage eventMessage = createEventMessage(record, correlationId);
            
            // Use circuit breaker and retry logic for the entire processing pipeline
            return circuitBreakerService.executeWithCircuitBreaker(
                "message-processing-" + topicName,
                () -> retryService.executeWithRetry(
                    topicName,
                    topicConfig,
                    () -> processMessageWithValidation(eventMessage, topicConfig),
                    "message-processing"
                )
            );
            
        } catch (CircuitBreakerService.CircuitBreakerOpenException e) {
            logger.warn("Circuit breaker is open for topic: {}. Message will be sent to DLQ", record.topic());
            EventMessage eventMessage = createEventMessage(record, correlationId);
            deadLetterQueueService.sendToDeadLetterQueue(eventMessage, findTopicConfig(record.topic()), 
                "Circuit breaker open", e);
            return createFailureResult(record.topic(), "Circuit breaker open");
            
        } catch (Exception e) {
            logger.error("Error processing message: {}", e.getMessage(), e);
            EventMessage eventMessage = createEventMessage(record, correlationId);
            deadLetterQueueService.sendToDeadLetterQueue(eventMessage, findTopicConfig(record.topic()), 
                "Processing failed after retries", e);
            return createFailureResult(record.topic(), e.getMessage());
        } finally {
            MDC.remove("correlationId");
        }
    }

    private ProcessingResult processMessageWithValidation(EventMessage eventMessage, TopicConfig topicConfig) {
        String topicName = eventMessage.getTopicName();
        
        // Validate schema
        if (!schemaValidationService.validateMessage(topicConfig.getSchemaFile(), eventMessage.getValue())) {
            logger.warn("Schema validation failed for topic: {}", topicName);
            throw new RuntimeException("Schema validation failed");
        }

        // Transform message
        Map<String, Object> transformedMessage = messageTransformationService.transform(
            eventMessage, topicConfig
        );

        // Write to S3
        deltaWriterService.writeMessage(transformedMessage, topicConfig);
        
        updateTopicStats(topicName, true);
        return createSuccessResult(topicName);
    }

    private TopicConfig findTopicConfig(String topicName) {
        return connectorConfiguration.getTopics().values().stream()
                .filter(config -> config.getKafkaTopic().equals(topicName))
                .findFirst()
                .orElse(null);
    }

    private EventMessage createEventMessage(ConsumerRecord<String, String> record, String correlationId) {
        EventMessage eventMessage = new EventMessage();
        eventMessage.setTopicName(record.topic());
        eventMessage.setPartition(String.valueOf(record.partition()));
        eventMessage.setOffset(record.offset());
        eventMessage.setKey(record.key());
        eventMessage.setValue(record.value());
        eventMessage.setTimestamp(LocalDateTime.now());
        eventMessage.setProcessedAt(LocalDateTime.now());
        
        return eventMessage;
    }

    private ProcessingResult createSuccessResult(String topicName) {
        ProcessingResult result = new ProcessingResult(true, topicName);
        result.setRecordsProcessed(1);
        result.markComplete();
        return result;
    }

    private ProcessingResult createFailureResult(String topicName, String errorMessage) {
        ProcessingResult result = new ProcessingResult(false, topicName);
        result.setErrorMessage(errorMessage);
        result.setRecordsFailed(1);
        result.markComplete();
        return result;
    }

    private void updateTopicStats(String topicName, boolean success) {
        topicStats.computeIfAbsent(topicName, k -> new ProcessingResult(true, k));
        ProcessingResult stats = topicStats.get(topicName);
        
        if (success) {
            stats.setRecordsProcessed(stats.getRecordsProcessed() + 1);
        } else {
            stats.setRecordsFailed(stats.getRecordsFailed() + 1);
        }
    }

    public Map<String, ProcessingResult> getTopicStats() {
        return new ConcurrentHashMap<>(topicStats);
    }
}