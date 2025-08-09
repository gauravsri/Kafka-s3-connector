package com.company.kafkaconnector.service;

import com.company.kafkaconnector.model.EventMessage;
import com.company.kafkaconnector.model.TopicConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Service
public class MessageTransformationService {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageTransformationService.class);
    private final ObjectMapper objectMapper;

    public MessageTransformationService() {
        this.objectMapper = new ObjectMapper();
    }

    public Map<String, Object> transform(EventMessage eventMessage, TopicConfig topicConfig) {
        try {
            logger.debug("Transforming message for topic: {}", eventMessage.getTopicName());
            
            // Parse the original JSON message
            Map<String, Object> originalData = parseJsonMessage(eventMessage.getValue());
            
            // Create the transformed message
            Map<String, Object> transformedData = new HashMap<>(originalData);
            
            // Add metadata fields
            addMetadataFields(transformedData, eventMessage);
            
            // Add partition values based on configuration
            addPartitionValues(transformedData, topicConfig);
            
            logger.debug("Message transformation completed for topic: {}", eventMessage.getTopicName());
            return transformedData;
            
        } catch (Exception e) {
            logger.error("Error transforming message for topic {}: {}", eventMessage.getTopicName(), e.getMessage(), e);
            throw new RuntimeException("Message transformation failed", e);
        }
    }

    private Map<String, Object> parseJsonMessage(String jsonMessage) throws IOException {
        return objectMapper.readValue(jsonMessage, new TypeReference<Map<String, Object>>() {});
    }

    private void addMetadataFields(Map<String, Object> data, EventMessage eventMessage) {
        // Add Kafka metadata
        data.put("_kafka_topic", eventMessage.getTopicName());
        data.put("_kafka_partition", eventMessage.getPartition());
        data.put("_kafka_offset", eventMessage.getOffset());
        data.put("_kafka_key", eventMessage.getKey());
        
        // Add processing metadata
        data.put("_processed_at", eventMessage.getProcessedAt().toString());
        data.put("_ingestion_timestamp", LocalDateTime.now().toString());
    }

    private void addPartitionValues(Map<String, Object> data, TopicConfig topicConfig) {
        LocalDateTime now = LocalDateTime.now();
        
        for (String partitionColumn : topicConfig.getDestination().getPartitionColumns()) {
            switch (partitionColumn.toLowerCase()) {
                case "year":
                    data.put("year", now.format(DateTimeFormatter.ofPattern("yyyy")));
                    break;
                case "month":
                    data.put("month", now.format(DateTimeFormatter.ofPattern("MM")));
                    break;
                case "day":
                    data.put("day", now.format(DateTimeFormatter.ofPattern("dd")));
                    break;
                case "hour":
                    data.put("hour", now.format(DateTimeFormatter.ofPattern("HH")));
                    break;
                default:
                    // For custom partition columns, try to extract from the data
                    if (!data.containsKey(partitionColumn)) {
                        // If not present, set a default value
                        data.put(partitionColumn, "unknown");
                        logger.debug("Added default value for partition column: {}", partitionColumn);
                    }
                    break;
            }
        }
    }

    /**
     * Apply field mappings if configured in the future
     */
    private void applyFieldMappings(Map<String, Object> data, TopicConfig topicConfig) {
        // Field mapping functionality would be implemented here
        // This would allow renaming fields, applying transformations, etc.
        logger.debug("Field mapping not yet configured for topic: {}", topicConfig.getKafkaTopic());
    }

    /**
     * Enrich data with additional fields if configured
     */
    private void enrichData(Map<String, Object> data, TopicConfig topicConfig) {
        // Data enrichment functionality would be implemented here
        // This could add lookup data, computed fields, etc.
        logger.debug("Data enrichment not yet configured for topic: {}", topicConfig.getKafkaTopic());
    }

    /**
     * Handle data type conversions for specific storage formats
     */
    private void normalizeDataTypes(Map<String, Object> data) {
        // Data type normalization would be implemented here
        // This would ensure consistent data types for storage
        // Convert types that might not be compatible with Delta Lake/Parquet
    }
}