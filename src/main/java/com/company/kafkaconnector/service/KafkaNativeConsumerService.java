package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.NonRetriableException;
import com.company.kafkaconnector.exception.RetriableException;
import com.company.kafkaconnector.model.TopicConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.UUID;

/**
 * Simplified Kafka consumer that relies on Kafka's native offset management for idempotency.
 * 
 * Key principles:
 * 1. Kafka Consumer Groups ensure only one instance processes each partition
 * 2. Manual acknowledgment ensures offsets are only committed after successful processing
 * 3. Restart recovery is automatic via Kafka's __consumer_offsets topic
 * 4. Idempotency is achieved through business logic being inherently idempotent
 */
@Service
public class KafkaNativeConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaNativeConsumerService.class);
    
    private final MultiFormatMessageParser messageParser;
    private final IdempotentDeltaProcessor deltaProcessor;
    private final TopicConfigurationService topicConfigService;
    private final DeadLetterQueueService dlqService;
    
    public KafkaNativeConsumerService(
            MultiFormatMessageParser messageParser,
            IdempotentDeltaProcessor deltaProcessor,
            TopicConfigurationService topicConfigService,
            DeadLetterQueueService dlqService) {
        this.messageParser = messageParser;
        this.deltaProcessor = deltaProcessor;
        this.topicConfigService = topicConfigService;
        this.dlqService = dlqService;
    }
    
    /**
     * Main Kafka message handler with built-in idempotency via offset management.
     * 
     * Kafka guarantees:
     * - Only one consumer instance per partition (no concurrent processing)
     * - Offsets stored in durable __consumer_offsets topic
     * - Automatic restart recovery from last committed offset
     * - Rebalancing handles instance failures gracefully
     */
    @KafkaListener(
        topics = "#{@connectorConfiguration.getTopics().values().![kafkaTopic]}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Retryable(
        value = {RetriableException.class}, 
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public void processMessage(
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        @Payload String rawMessage,
        Acknowledgment acknowledgment,
        Consumer<String, String> consumer
    ) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        MDC.put("topic", topic);
        MDC.put("partition", String.valueOf(partition));
        MDC.put("offset", String.valueOf(offset));
        
        logger.debug("Processing message - Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
        
        try {
            // 1. Get topic configuration
            TopicConfig topicConfig = topicConfigService.getConfigForTopic(topic);
            if (topicConfig == null) {
                throw new NonRetriableException("No configuration found for topic: " + topic);
            }
            
            // 2. Parse message to standard format (handles JSON/CSV/Avro)
            GenericRecord avroRecord = messageParser.parseMessage(rawMessage, topic, topicConfig);
            
            // 3. Extract COB for partitioning
            String cob = extractCobFromRecord(avroRecord);
            
            // 4. Process message idempotently using business logic processor
            deltaProcessor.processIdempotently(topic, cob, rawMessage, avroRecord, topicConfig);
            
            // 5. Commit offset ONLY after successful processing
            // This ensures we don't lose messages and provides natural idempotency
            acknowledgment.acknowledge();
            
            logger.debug("Successfully processed and committed - Topic: {}, Partition: {}, Offset: {}", 
                        topic, partition, offset);
            
        } catch (RetriableException e) {
            logger.warn("Retriable error processing message - Topic: {}, Partition: {}, Offset: {} - will retry", 
                       topic, partition, offset, e);
            
            // Don't acknowledge - Kafka will retry from this offset
            // Spring Retry will handle the retry logic
            throw e;
            
        } catch (NonRetriableException e) {
            logger.error("Non-retriable error - Topic: {}, Partition: {}, Offset: {} - sending to DLQ", 
                        topic, partition, offset, e);
            
            handleNonRetriableFailure(topic, rawMessage, e, acknowledgment);
            
        } catch (Exception e) {
            logger.error("Unexpected error - Topic: {}, Partition: {}, Offset: {}", 
                        topic, partition, offset, e);
            
            // Classify unknown exceptions
            if (isRetriableException(e)) {
                throw new RetriableException("Unexpected retriable error", e);
            } else {
                handleNonRetriableFailure(topic, rawMessage, 
                    new NonRetriableException("Unexpected non-retriable error", e), acknowledgment);
            }
            
        } finally {
            MDC.clear();
        }
    }
    
    
    private String extractCobFromRecord(GenericRecord record) {
        Object cobValue = record.get("cob");
        if (cobValue == null) {
            throw new NonRetriableException("COB field is required but missing");
        }
        
        // Basic format check - detailed validation is done in IdempotentDeltaProcessor
        String cob = cobValue.toString();
        try {
            LocalDate.parse(cob, DateTimeFormatter.ISO_LOCAL_DATE);
            return cob;
        } catch (DateTimeParseException e) {
            throw new NonRetriableException("Invalid COB date format: " + cob, e);
        }
    }
    
    
    private boolean isRetriableException(Exception e) {
        return e instanceof java.net.SocketTimeoutException ||
               e instanceof java.io.IOException ||
               (e.getMessage() != null && e.getMessage().toLowerCase().contains("timeout"));
    }
    
    private void handleNonRetriableFailure(String topic, String rawMessage, 
                                         Exception e, Acknowledgment acknowledgment) {
        try {
            // Send to dead letter queue
            dlqService.sendToDeadLetterQueue(topic, null, rawMessage, e.getMessage(), e);
            
            // Acknowledge to move past this message
            acknowledgment.acknowledge();
            
            logger.info("Sent message to DLQ and acknowledged - Topic: {}", topic);
            
        } catch (Exception dlqException) {
            logger.error("Failed to send to DLQ - Topic: {} - acknowledging anyway to prevent infinite loop", 
                        topic, dlqException);
            
            // Still acknowledge to prevent infinite reprocessing
            acknowledgment.acknowledge();
        }
    }
}