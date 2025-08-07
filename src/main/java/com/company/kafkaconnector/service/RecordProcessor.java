package com.company.kafkaconnector.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Record processor for transforming and enriching sink records
 * Following Single Responsibility Principle
 */
@Slf4j
public class RecordProcessor {
    
    /**
     * Process a sink record by adding metadata and performing transformations
     * Following Open/Closed Principle - extensible without modification
     */
    public SinkRecord process(SinkRecord record) {
        log.trace("Processing record from topic {} partition {} offset {}", 
                 record.topic(), record.kafkaPartition(), record.kafkaOffset());
        
        // For now, just add processing metadata
        // This can be extended to include schema transformation, data enrichment, etc.
        return addMetadata(record);
    }
    
    /**
     * Add processing metadata to the record
     * Following the decorator pattern
     */
    private SinkRecord addMetadata(SinkRecord record) {
        try {
            // Create enhanced value with metadata
            Map<String, Object> enhancedValue = new HashMap<>();
            
            // Add original data
            if (record.value() != null) {
                if (record.value() instanceof Map) {
                    enhancedValue.putAll((Map<String, Object>) record.value());
                } else if (record.value() instanceof Struct) {
                    enhancedValue.putAll(structToMap((Struct) record.value()));
                } else {
                    enhancedValue.put("original_value", record.value());
                }
            }
            
            // Add metadata
            Map<String, Object> metadata = createMetadata(record);
            enhancedValue.put("_metadata", metadata);
            
            // Create new record with enhanced value
            return new SinkRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                createEnhancedSchema(record.valueSchema()),
                enhancedValue,
                record.kafkaOffset(),
                record.timestamp(),
                record.timestampType()
            );
            
        } catch (Exception e) {
            log.warn("Failed to add metadata to record, returning original: {}", e.getMessage());
            return record;
        }
    }
    
    /**
     * Create metadata map for the record
     */
    private Map<String, Object> createMetadata(SinkRecord record) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("processing_timestamp", Instant.now().toString());
        metadata.put("kafka_topic", record.topic());
        metadata.put("kafka_partition", record.kafkaPartition());
        metadata.put("kafka_offset", record.kafkaOffset());
        metadata.put("kafka_timestamp", record.timestamp());
        metadata.put("processor_version", "1.0.0");
        
        return metadata;
    }
    
    /**
     * Convert Struct to Map
     */
    private Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> map = new HashMap<>();
        
        if (struct.schema() != null) {
            struct.schema().fields().forEach(field -> {
                Object value = struct.get(field);
                if (value != null) {
                    map.put(field.name(), value);
                }
            });
        }
        
        return map;
    }
    
    /**
     * Create enhanced schema with metadata field
     */
    private Schema createEnhancedSchema(Schema originalSchema) {
        if (originalSchema == null) {
            return null;
        }
        
        SchemaBuilder builder = SchemaBuilder.struct()
            .name(originalSchema.name() + "_enhanced")
            .version(originalSchema.version())
            .doc("Enhanced schema with processing metadata");
        
        // Add original fields
        if (originalSchema.type() == Schema.Type.STRUCT) {
            originalSchema.fields().forEach(field -> {
                builder.field(field.name(), field.schema());
            });
        } else {
            builder.field("original_value", originalSchema);
        }
        
        // Add metadata field
        Schema metadataSchema = SchemaBuilder.struct()
            .field("processing_timestamp", Schema.STRING_SCHEMA)
            .field("kafka_topic", Schema.STRING_SCHEMA)
            .field("kafka_partition", Schema.INT32_SCHEMA)
            .field("kafka_offset", Schema.INT64_SCHEMA)
            .field("kafka_timestamp", Schema.OPTIONAL_INT64_SCHEMA)
            .field("processor_version", Schema.STRING_SCHEMA)
            .build();
            
        builder.field("_metadata", metadataSchema);
        
        return builder.build();
    }
}