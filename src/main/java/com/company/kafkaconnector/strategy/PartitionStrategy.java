package com.company.kafkaconnector.strategy;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Strategy interface for partitioning records
 * Following Strategy Pattern
 */
public interface PartitionStrategy {
    
    /**
     * Generate partition key for the given record
     * @param record The sink record to partition
     * @return Partition key string used for organizing data in S3
     */
    String getPartitionKey(SinkRecord record);
    
    /**
     * Get the partitioning scheme name
     * @return Name of the partitioning strategy
     */
    String getStrategyName();
}