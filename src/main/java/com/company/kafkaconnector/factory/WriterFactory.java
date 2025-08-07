package com.company.kafkaconnector.factory;

import com.company.kafkaconnector.writer.S3Writer;
import com.company.kafkaconnector.writer.S3DeltaWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Factory for creating S3 writers
 * Following Factory Pattern and Single Responsibility Principle
 */
@Slf4j
public class WriterFactory {
    
    private final Map<String, String> config;
    
    public WriterFactory(Map<String, String> config) {
        this.config = config;
    }
    
    /**
     * Creates appropriate writer based on configuration and partition key
     * Following Factory Method Pattern
     */
    public S3Writer createWriter(String partitionKey) {
        log.debug("Creating writer for partition key: {}", partitionKey);
        
        // For now, always create S3DeltaWriter
        // This can be extended to support different writer types based on configuration
        return createS3DeltaWriter(partitionKey);
    }
    
    /**
     * Creates S3 Delta writer with proper configuration
     * Following Builder Pattern for complex object creation
     */
    private S3Writer createS3DeltaWriter(String partitionKey) {
        return S3DeltaWriter.builder()
            .config(config)
            .partitionKey(partitionKey)
            .bucketName(config.get("s3.bucket.name"))
            .region(config.getOrDefault("s3.region", "us-east-1"))
            .accessKeyId(config.get("s3.access.key.id"))
            .secretAccessKey(config.get("s3.secret.access.key"))
            .endpointUrl(config.get("s3.endpoint.url"))
            .topicsDir(config.getOrDefault("topics.dir", "topics"))
            .build();
    }
    
    /**
     * Validates writer configuration
     * Following validation pattern
     */
    private void validateWriterConfig(Map<String, String> config) {
        if (StringUtils.isBlank(config.get("s3.bucket.name"))) {
            throw new IllegalArgumentException("S3 bucket name is required");
        }
        
        // Add more validation as needed
        log.debug("Writer configuration validated successfully");
    }
}