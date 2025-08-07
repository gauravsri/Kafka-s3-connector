package com.company.kafkaconnector.strategy;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Topic and Kafka partition based strategy
 * Creates partitions: topic/kafka_partition=X
 */
@Slf4j
public class TopicPartitionStrategy implements PartitionStrategy {
    
    private final Map<String, String> config;
    
    public TopicPartitionStrategy(Map<String, String> config) {
        this.config = config;
    }
    
    @Override
    public String getPartitionKey(SinkRecord record) {
        return String.format("%s/kafka_partition=%d", record.topic(), record.kafkaPartition());
    }
    
    @Override
    public String getStrategyName() {
        return "TOPIC_PARTITION";
    }
}