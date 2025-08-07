package com.company.kafkaconnector.strategy;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Default partitioning strategy using topic and kafka partition
 * Following Strategy Pattern implementation
 */
@Slf4j
public class DefaultPartitionStrategy implements PartitionStrategy {
    
    private final Map<String, String> config;
    
    public DefaultPartitionStrategy(Map<String, String> config) {
        this.config = config;
    }
    
    @Override
    public String getPartitionKey(SinkRecord record) {
        // Create partition key: topic/partition=X
        return String.format("%s/partition=%d", record.topic(), record.kafkaPartition());
    }
    
    @Override
    public String getStrategyName() {
        return "DEFAULT";
    }
}