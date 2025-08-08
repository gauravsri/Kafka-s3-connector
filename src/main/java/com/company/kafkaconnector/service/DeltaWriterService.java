package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.DeltaWriteException;
import com.company.kafkaconnector.model.TopicConfig;
import io.delta.kernel.types.*;
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

    private final DeltaKernelWriterService deltaKernelWriterService;
    private final DeltaOptimizationService deltaOptimizationService;
    private final Map<String, List<Map<String, Object>>> messageBatches = new ConcurrentHashMap<>();
    private final Map<String, Integer> batchCounts = new ConcurrentHashMap<>();
    private final Map<String, TopicConfig> destinationTopicConfigs = new ConcurrentHashMap<>();

    public DeltaWriterService(DeltaKernelWriterService deltaKernelWriterService, 
                             DeltaOptimizationService deltaOptimizationService) {
        this.deltaKernelWriterService = deltaKernelWriterService;
        this.deltaOptimizationService = deltaOptimizationService;
    }

    public void writeMessage(Map<String, Object> transformedMessage, TopicConfig topicConfig) {
        try {
            String destinationKey = buildDestinationKey(topicConfig);
            
            // Store topic config for later use in flushAllBatches
            destinationTopicConfigs.put(destinationKey, topicConfig);
            
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

        StructType schema = createSchema(batch.get(0));
        deltaKernelWriterService.write(topicConfig, batch, schema);

        logger.info("Successfully wrote batch to Delta Lake: {}", destinationKey);

        // Track batch for optimization and trigger optimizations if needed
        String tablePath = String.format("s3a://%s/%s",
                topicConfig.getDestination().getBucket(),
                topicConfig.getDestination().getPath());
        
        deltaOptimizationService.trackBatchWrite(tablePath);
        
        clearBatch(destinationKey);
        handleOptimizations(topicConfig);
    }

    private StructType createSchema(Map<String, Object> message) {
        StructType schema = new StructType();
        for (Map.Entry<String, Object> entry : message.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            DataType dataType = getDataType(value);
            schema = schema.add(key, dataType);
        }
        return schema;
    }

    private DataType getDataType(Object value) {
        if (value instanceof String) {
            return StringType.STRING;
        } else if (value instanceof Integer) {
            return IntegerType.INTEGER;
        } else if (value instanceof Long) {
            return LongType.LONG;
        } else if (value instanceof Double) {
            return DoubleType.DOUBLE;
        } else if (value instanceof Boolean) {
            return BooleanType.BOOLEAN;
        } else {
            throw new UnsupportedOperationException("Unsupported data type: " + value.getClass().getName());
        }
    }

    private void handleOptimizations(TopicConfig topicConfig) {
        try {
            deltaOptimizationService.performOptimizations(topicConfig);
        } catch (Exception e) {
            logger.warn("Error during optimization for topic {}: {}", 
                topicConfig.getKafkaTopic(), e.getMessage(), e);
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

    private void clearBatch(String destinationKey) {
        messageBatches.remove(destinationKey);
        batchCounts.remove(destinationKey);
    }

    public void flushAllBatches() {
        logger.info("Flushing all pending batches...");
        
        int totalFlushed = 0;
        int errors = 0;
        
        for (Map.Entry<String, List<Map<String, Object>>> entry : messageBatches.entrySet()) {
            String destinationKey = entry.getKey();
            List<Map<String, Object>> batch = entry.getValue();
            TopicConfig topicConfig = destinationTopicConfigs.get(destinationKey);
            
            if (batch != null && !batch.isEmpty() && topicConfig != null) {
                try {
                    logger.info("Flushing batch for destination: {} ({} messages)", destinationKey, batch.size());
                    flushBatch(destinationKey, topicConfig);
                    totalFlushed += batch.size();
                } catch (Exception e) {
                    logger.error("Failed to flush batch for destination: {}", destinationKey, e);
                    errors++;
                }
            }
        }
        
        logger.info("Flush all batches completed: {} messages flushed, {} errors", totalFlushed, errors);
        
        if (errors > 0) {
            logger.warn("Some batches failed to flush. Check logs for details.");
        }
    }
}
