package com.company.kafkaconnector.strategy;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Factory for creating partition strategies
 * Following Factory Pattern and Open/Closed Principle
 */
@Slf4j
public class PartitionStrategyFactory {
    
    public static final String DEFAULT_STRATEGY = "DEFAULT";
    public static final String TIME_BASED_STRATEGY = "TIME_BASED";
    public static final String TOPIC_PARTITION_STRATEGY = "TOPIC_PARTITION";
    
    /**
     * Creates appropriate partitioning strategy based on configuration
     */
    public static PartitionStrategy createStrategy(Map<String, String> config) {
        String strategyType = config.getOrDefault("partitioner.strategy", DEFAULT_STRATEGY);
        
        log.info("Creating partition strategy: {}", strategyType);
        
        switch (strategyType.toUpperCase()) {
            case TIME_BASED_STRATEGY:
                return new TimeBasedPartitionStrategy(config);
            case TOPIC_PARTITION_STRATEGY:
                return new TopicPartitionStrategy(config);
            case DEFAULT_STRATEGY:
            default:
                return new DefaultPartitionStrategy(config);
        }
    }
}