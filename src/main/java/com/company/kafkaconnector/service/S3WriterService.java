package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.DeltaWriteException;
import com.company.kafkaconnector.model.TopicConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class S3WriterService {
    
    private static final Logger logger = LoggerFactory.getLogger(S3WriterService.class);
    
    private final S3Client s3Client;
    private final ObjectMapper objectMapper;
    
    // TODO: Replace with proper Delta Lake implementation
    // For now, we'll batch messages and write as JSON files to S3
    private final Map<String, List<Map<String, Object>>> messageBatches = new ConcurrentHashMap<>();
    private final Map<String, Integer> batchCounts = new ConcurrentHashMap<>();

    @Autowired
    public S3WriterService(S3Client s3Client) {
        this.s3Client = s3Client;
        this.objectMapper = new ObjectMapper();
    }

    public boolean writeMessage(Map<String, Object> transformedMessage, TopicConfig topicConfig) {
        try {
            String destinationKey = buildDestinationKey(topicConfig);
            
            // Add to batch
            addToBatch(destinationKey, transformedMessage);
            
            // Check if we should flush the batch
            if (shouldFlushBatch(destinationKey, topicConfig)) {
                return flushBatch(destinationKey, topicConfig);
            }
            
            return true;
            
        } catch (Exception e) {
            logger.error("Error writing message to S3 for topic {}: {}", 
                topicConfig.getKafkaTopic(), e.getMessage(), e);
            throw new DeltaWriteException(
                "Failed to write message to S3", 
                topicConfig.getKafkaTopic(),
                UUID.randomUUID().toString(),
                topicConfig.getDestination().getFullPath(),
                1,
                e
            );
        }
    }

    private String buildDestinationKey(TopicConfig topicConfig) {
        return topicConfig.getKafkaTopic() + "_" + topicConfig.getDestination().getTableName();
    }

    private void addToBatch(String destinationKey, Map<String, Object> message) {
        messageBatches.computeIfAbsent(destinationKey, k -> new ArrayList<>()).add(message);
        batchCounts.merge(destinationKey, 1, Integer::sum);
    }

    private boolean shouldFlushBatch(String destinationKey, TopicConfig topicConfig) {
        int currentBatchSize = batchCounts.getOrDefault(destinationKey, 0);
        return currentBatchSize >= topicConfig.getProcessing().getBatchSize();
    }

    private boolean flushBatch(String destinationKey, TopicConfig topicConfig) {
        try {
            List<Map<String, Object>> batch = messageBatches.get(destinationKey);
            if (batch == null || batch.isEmpty()) {
                return true;
            }

            logger.info("Flushing batch of {} messages for destination: {}", batch.size(), destinationKey);

            // Generate S3 key with timestamp and batch ID
            String s3Key = generateS3Key(topicConfig, batch.size());
            
            // Convert batch to JSON
            String batchJson = objectMapper.writeValueAsString(batch);
            
            // Write to S3
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(topicConfig.getDestination().getBucket())
                    .key(s3Key)
                    .contentType("application/json")
                    .build();

            PutObjectResponse response = s3Client.putObject(putRequest, RequestBody.fromString(batchJson));
            
            logger.info("Successfully wrote batch to S3: s3://{}/{}", 
                topicConfig.getDestination().getBucket(), s3Key);
            
            // Clear the batch
            clearBatch(destinationKey);
            
            return response.sdkHttpResponse().isSuccessful();
            
        } catch (Exception e) {
            logger.error("Error flushing batch for destination {}: {}", destinationKey, e.getMessage(), e);
            return false;
        }
    }

    private String generateS3Key(TopicConfig topicConfig, int batchSize) {
        LocalDateTime now = LocalDateTime.now();
        String timestamp = now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd/HH"));
        String batchId = UUID.randomUUID().toString().substring(0, 8);
        
        return String.format("%s/%s/batch_%s_%d_records.json", 
            topicConfig.getDestination().getPath(),
            timestamp,
            batchId,
            batchSize
        );
    }

    private void clearBatch(String destinationKey) {
        messageBatches.remove(destinationKey);
        batchCounts.remove(destinationKey);
    }

    /**
     * Force flush all pending batches (useful for shutdown or manual triggers)
     */
    public void flushAllBatches() {
        logger.info("Flushing all pending batches...");
        
        for (String destinationKey : messageBatches.keySet()) {
            // TODO: Need TopicConfig here - implement batch metadata storage
            logger.warn("Cannot flush batch {} - need TopicConfig implementation", destinationKey);
        }
    }

    /**
     * Get current batch statistics
     */
    public Map<String, Integer> getBatchStatistics() {
        return new ConcurrentHashMap<>(batchCounts);
    }

    /**
     * TODO: Implement proper Delta Lake integration
     * This method would be replaced with Delta Lake table operations:
     * - Create/update Delta table schema
     * - Write data with ACID guarantees
     * - Handle schema evolution
     * - Implement optimize and vacuum operations
     */
    private void writeDeltaLakeTable(List<Map<String, Object>> batch, TopicConfig topicConfig) {
        // Placeholder for Delta Lake implementation
        throw new UnsupportedOperationException("Delta Lake integration not yet implemented");
    }
}