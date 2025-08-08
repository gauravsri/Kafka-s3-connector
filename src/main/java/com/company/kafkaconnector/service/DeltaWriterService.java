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
    private final Map<String, List<Map<String, Object>>> messageBatches = new ConcurrentHashMap<>();
    private final Map<String, Integer> batchCounts = new ConcurrentHashMap<>();

    public DeltaWriterService(DeltaKernelWriterService deltaKernelWriterService) {
        this.deltaKernelWriterService = deltaKernelWriterService;
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

        StructType schema = createSchema(batch.get(0));
        deltaKernelWriterService.write(topicConfig, batch, schema);

        logger.info("Successfully wrote batch to Delta Lake: {}", destinationKey);

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
