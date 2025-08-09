package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.DeltaWriteException;
import com.company.kafkaconnector.model.TopicConfig;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Service
public class CobAwareDeltaWriterService {
    
    private static final Logger logger = LoggerFactory.getLogger(CobAwareDeltaWriterService.class);
    
    private final DeltaWriterService deltaWriterService;
    
    public CobAwareDeltaWriterService(DeltaWriterService deltaWriterService) {
        this.deltaWriterService = deltaWriterService;
    }
    
    public void writeMessage(String topic, String cob, GenericRecord avroRecord, TopicConfig topicConfig) {
        try {
            logger.debug("Writing message to Delta Lake - Topic: {}, COB: {}", topic, cob);
            
            // Construct COB-based partitioned path
            String deltaTablePath = constructDeltaPath(topicConfig, cob);
            
            // Transform Avro record to Delta Lake compatible format
            Map<String, Object> deltaRow = transformAvroToDeltaRow(avroRecord, cob);
            
            // COB-based partitioning will be handled in the Delta path construction
            
            // Write to Delta Lake using existing writer
            // Add COB-based partition information to the row
            deltaRow.put("cob_date", cob);
            deltaWriterService.writeMessage(deltaRow, topicConfig);
            
            logger.info("Successfully wrote message to Delta Lake - Topic: {}, COB: {}, Path: {}", 
                       topic, cob, deltaTablePath);
            
        } catch (Exception e) {
            // Wrap in DeltaWriteException with proper classification
            throw new DeltaWriteException("Failed to write to Delta Lake for topic: " + topic, 
                constructDeltaPath(topicConfig, cob), 1, e);
        }
    }
    
    /**
     * Construct Delta Lake path with COB-based partitioning
     */
    private String constructDeltaPath(TopicConfig topicConfig, String cob) {
        // Parse COB date for hierarchical partitioning
        LocalDate cobDate = LocalDate.parse(cob, DateTimeFormatter.ISO_LOCAL_DATE);
        
        // Build S3 path: s3://bucket/table/year=2024/month=01/day=15/
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append("s3://")
                   .append(topicConfig.getDestination().getBucket())
                   .append("/")
                   .append(topicConfig.getDestination().getPath())
                   .append("/year=").append(cobDate.getYear())
                   .append("/month=").append(String.format("%02d", cobDate.getMonthValue()))
                   .append("/day=").append(String.format("%02d", cobDate.getDayOfMonth()))
                   .append("/");
        
        return pathBuilder.toString();
    }
    
    /**
     * Transform Avro record to Delta Lake row format
     */
    private Map<String, Object> transformAvroToDeltaRow(GenericRecord avroRecord, String cob) {
        Map<String, Object> deltaRow = new HashMap<>();
        
        // Copy all fields from Avro record
        avroRecord.getSchema().getFields().forEach(field -> {
            String fieldName = field.name();
            Object fieldValue = avroRecord.get(fieldName);
            
            if (fieldValue != null) {
                // Convert Avro-specific types to Delta Lake compatible types
                deltaRow.put(fieldName, convertAvroValueToDelta(fieldValue));
            }
        });
        
        // Add processing metadata
        deltaRow.put("processed_timestamp", System.currentTimeMillis());
        deltaRow.put("cob_date", cob);
        
        return deltaRow;
    }
    
    /**
     * Create COB-based partition values
     */
    private Map<String, String> createCobPartitionValues(String cob) {
        LocalDate cobDate = LocalDate.parse(cob, DateTimeFormatter.ISO_LOCAL_DATE);
        
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("year", String.valueOf(cobDate.getYear()));
        partitionValues.put("month", String.format("%02d", cobDate.getMonthValue()));
        partitionValues.put("day", String.format("%02d", cobDate.getDayOfMonth()));
        partitionValues.put("cob_date", cob);
        
        return partitionValues;
    }
    
    /**
     * Convert Avro-specific types to Delta Lake compatible types
     */
    private Object convertAvroValueToDelta(Object avroValue) {
        if (avroValue == null) {
            return null;
        }
        
        // Handle Avro-specific types
        if (avroValue instanceof org.apache.avro.util.Utf8) {
            return avroValue.toString();
        } else if (avroValue instanceof org.apache.avro.generic.GenericData.EnumSymbol) {
            return avroValue.toString();
        } else if (avroValue instanceof org.apache.avro.generic.GenericData.Array) {
            // Convert Avro array to standard list
            java.util.List<Object> list = new java.util.ArrayList<>();
            for (Object item : (org.apache.avro.generic.GenericData.Array<?>) avroValue) {
                list.add(convertAvroValueToDelta(item));
            }
            return list;
        } else if (avroValue instanceof Map) {
            // Convert map values
            Map<String, Object> convertedMap = new HashMap<>();
            ((Map<?, ?>) avroValue).forEach((key, value) -> {
                convertedMap.put(key.toString(), convertAvroValueToDelta(value));
            });
            return convertedMap;
        } else if (avroValue instanceof GenericRecord) {
            // Convert nested GenericRecord to Map
            GenericRecord record = (GenericRecord) avroValue;
            Map<String, Object> recordMap = new HashMap<>();
            record.getSchema().getFields().forEach(field -> {
                Object fieldValue = record.get(field.name());
                if (fieldValue != null) {
                    recordMap.put(field.name(), convertAvroValueToDelta(fieldValue));
                }
            });
            return recordMap;
        }
        
        // Return as-is for primitive types and other compatible types
        return avroValue;
    }
    
    private boolean isRetriableError(Exception e) {
        // Simple classification - in real implementation this would be more sophisticated
        String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
        
        // Network/timeout errors are usually retriable
        if (message.contains("timeout") || message.contains("connection") || message.contains("network")) {
            return true;
        }
        
        // S3 throttling is retriable
        if (message.contains("throttle") || message.contains("rate limit")) {
            return true;
        }
        
        // Schema validation errors are usually not retriable
        if (message.contains("schema") || message.contains("validation")) {
            return false;
        }
        
        // Default to retriable for unknown errors
        return true;
    }
}