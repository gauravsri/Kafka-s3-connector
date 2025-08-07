package com.company.kafkaconnector.service;

import com.company.kafkaconnector.config.ConnectorConfiguration;
import com.company.kafkaconnector.model.ProcessingResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;

@Service
public class KafkaConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    
    private final TopicRouterService topicRouterService;
    private final ConnectorConfiguration connectorConfiguration;

    @Autowired
    public KafkaConsumerService(TopicRouterService topicRouterService, 
                               ConnectorConfiguration connectorConfiguration) {
        this.topicRouterService = topicRouterService;
        this.connectorConfiguration = connectorConfiguration;
    }

    @PostConstruct
    public void init() {
        logger.info("Kafka Consumer Service initialized");
        logger.info("Configured topics: {}", 
            connectorConfiguration.getTopics().keySet());
    }

    /**
     * Generic Kafka listener that handles messages from configured topics.
     * The topics are dynamically determined from the configuration.
     */
    @KafkaListener(topics = "#{@connectorConfiguration.getTopics().values().![kafkaTopic]}", 
                   groupId = "#{@environment.getProperty('spring.kafka.consumer.group-id')}")
    public void handleMessage(
            ConsumerRecord<String, String> record,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {
        
        logger.debug("Received message from topic: {}, partition: {}, offset: {}", 
            topic, partition, offset);
        
        try {
            ProcessingResult result = topicRouterService.processMessage(record);
            
            if (result.isSuccess()) {
                logger.debug("Successfully processed message from topic: {}", topic);
                acknowledgment.acknowledge();
            } else {
                logger.error("Failed to process message from topic: {}. Error: {}", 
                    topic, result.getErrorMessage());
                
                // TODO: Implement retry logic and dead letter queue
                // For now, we acknowledge to prevent infinite reprocessing
                acknowledgment.acknowledge();
            }
            
        } catch (Exception e) {
            logger.error("Unexpected error processing message from topic: {}", topic, e);
            
            // TODO: Implement proper error handling strategy
            acknowledgment.acknowledge();
        }
    }

    /**
     * Batch message handler (alternative to single message processing)
     * This can be enabled for higher throughput scenarios
     */
    // @KafkaListener(topics = "#{@connectorConfiguration.getTopics().values().![kafkaTopic]}", 
    //                groupId = "#{@environment.getProperty('spring.kafka.consumer.group-id')}")
    public void handleBatchMessages(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment) {
        
        logger.debug("Received batch of {} messages", records.size());
        
        int successCount = 0;
        int failureCount = 0;
        
        for (ConsumerRecord<String, String> record : records) {
            try {
                ProcessingResult result = topicRouterService.processMessage(record);
                
                if (result.isSuccess()) {
                    successCount++;
                } else {
                    failureCount++;
                    logger.error("Failed to process message: {}", result.getErrorMessage());
                }
                
            } catch (Exception e) {
                failureCount++;
                logger.error("Error processing message from topic: {}", record.topic(), e);
            }
        }
        
        logger.info("Batch processing completed. Success: {}, Failures: {}", 
            successCount, failureCount);
        
        acknowledgment.acknowledge();
    }

    /**
     * Handle specific topic listeners if dynamic configuration doesn't work
     * These would be created based on the configuration
     */
    
    @KafkaListener(topics = "user.events.v1", groupId = "kafka-s3-connector")
    public void handleUserEvents(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        handleMessage(record, record.topic(), record.partition(), record.offset(), acknowledgment);
    }

    @KafkaListener(topics = "orders.lifecycle.v2", groupId = "kafka-s3-connector")
    public void handleOrderEvents(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        handleMessage(record, record.topic(), record.partition(), record.offset(), acknowledgment);
    }

    /**
     * Get consumer statistics and health information
     */
    public boolean isHealthy() {
        // TODO: Implement health check logic
        // Check if consumers are active, no major errors, etc.
        return true;
    }
}