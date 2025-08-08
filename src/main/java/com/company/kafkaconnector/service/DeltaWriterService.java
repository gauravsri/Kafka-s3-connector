package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.DeltaWriteException;
import com.company.kafkaconnector.model.TopicConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DeltaWriterService {

    private static final Logger logger = LoggerFactory.getLogger(DeltaWriterService.class);

    private final SparkSession sparkSession;
    private final ObjectMapper objectMapper;
    private final Map<String, List<Map<String, Object>>> messageBatches = new ConcurrentHashMap<>();
    private final Map<String, Integer> batchCounts = new ConcurrentHashMap<>();

    public DeltaWriterService(SparkSession sparkSession, ObjectMapper objectMapper) {
        this.sparkSession = sparkSession;
        this.objectMapper = objectMapper;
    }

    public void writeMessage(Map<String, Object> transformedMessage, TopicConfig topicConfig) {
        try {
            String destinationKey = buildDestinationKey(topicConfig);
            addToBatch(destinationKey, transformedMessage);

            if (shouldFlushBatch(destinationKey, topicConfig)) {
                flushBatch(destinationKey, topicConfig);
            }
        } catch (Exception e) {
            logger.error("Error writing message to Delta Lake for topic {}: {}",
                    topicConfig.getKafkaTopic(), e.getMessage(), e);
            throw new DeltaWriteException(
                    "Failed to write message to Delta Lake",
                    topicConfig.getKafkaTopic(),
                    UUID.randomUUID().toString(),
                    topicConfig.getDestination().getFullPath(),
                    1,
                    e
            );
        }
    }

    private void flushBatch(String destinationKey, TopicConfig topicConfig) throws IOException {
        List<Map<String, Object>> batch = messageBatches.get(destinationKey);
        if (batch == null || batch.isEmpty()) {
            return;
        }

        logger.info("Flushing batch of {} messages for destination: {}", batch.size(), destinationKey);

        String s3Path = String.format("s3a://%s/%s",
                topicConfig.getDestination().getBucket(),
                topicConfig.getDestination().getPath());

        String jsonRecords = objectMapper.writeValueAsString(batch);
        Dataset<Row> df = sparkSession.read().json(sparkSession.createDataset(
                List.of(jsonRecords),
                org.apache.spark.sql.Encoders.STRING()));

        df.write()
                .format("delta")
                .mode(SaveMode.Append)
                .option("mergeSchema", "true")
                .partitionBy(topicConfig.getDestination().getPartitionColumns())
                .save(s3Path);

        logger.info("Successfully wrote batch to Delta Lake: {}", s3Path);

        clearBatch(destinationKey);
        handleOptimizations(s3Path, topicConfig);
    }

    private void handleOptimizations(String s3Path, TopicConfig topicConfig) {
        // Placeholder for optimize/vacuum
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

    private void clearBatch(String destinationKey) {
        messageBatches.remove(destinationKey);
        batchCounts.remove(destinationKey);
    }

    public void flushAllBatches() {
        // This needs to be implemented properly, it needs access to TopicConfig for each destinationKey
        logger.warn("flushAllBatches is not fully implemented yet.");
    }
}
