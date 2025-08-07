package com.company.kafkaconnector.strategy;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for partition strategies
 */
class PartitionStrategyTest {

    @Test
    void defaultPartitionStrategy_shouldUseTopicAndKafkaPartition() {
        // Given
        PartitionStrategy strategy = new DefaultPartitionStrategy(Map.of());
        SinkRecord record = createSinkRecord("test-topic", 3, "key", "value");

        // When
        String partitionKey = strategy.getPartitionKey(record);

        // Then
        assertThat(partitionKey).isEqualTo("test-topic/partition=3");
    }

    @Test
    void timeBasedPartitionStrategy_shouldUseCurrentTime() {
        // Given
        PartitionStrategy strategy = new TimeBasedPartitionStrategy(Map.of());
        SinkRecord record = createSinkRecord("test-topic", 1, "key", "value");

        // When
        String partitionKey = strategy.getPartitionKey(record);

        // Then
        assertThat(partitionKey).startsWith("test-topic/year=");
        assertThat(partitionKey).contains("/month=");
        assertThat(partitionKey).contains("/day=");
        assertThat(partitionKey).contains("/hour=");
    }

    @Test
    void timeBasedPartitionStrategy_shouldHandleRecordTimestamp() {
        // Given
        PartitionStrategy strategy = new TimeBasedPartitionStrategy(Map.of());
        long timestamp = System.currentTimeMillis();
        SinkRecord record = createSinkRecordWithTimestamp("test-topic", 1, "key", "value", timestamp);

        // When
        String partitionKey = strategy.getPartitionKey(record);

        // Then
        assertThat(partitionKey).matches("test-topic/year=\\d{4}/month=\\d{2}/day=\\d{2}/hour=\\d{2}");
    }

    @Test
    void topicPartitionStrategy_shouldUseBothTopicAndPartition() {
        // Given
        PartitionStrategy strategy = new TopicPartitionStrategy(Map.of());
        SinkRecord record = createSinkRecord("user-events", 5, "user123", "data");

        // When
        String partitionKey = strategy.getPartitionKey(record);

        // Then
        assertThat(partitionKey).isEqualTo("user-events/partition=5");
    }

    @Test
    void partitionStrategyFactory_shouldCreateDefaultStrategy() {
        // Given
        Map<String, String> config = Map.of("partitioner.strategy", "DEFAULT");

        // When
        PartitionStrategy strategy = PartitionStrategyFactory.createStrategy(config);

        // Then
        assertThat(strategy).isInstanceOf(DefaultPartitionStrategy.class);
        assertThat(strategy.getStrategyName()).isEqualTo("DEFAULT");
    }

    @Test
    void partitionStrategyFactory_shouldCreateTimeBasedStrategy() {
        // Given
        Map<String, String> config = Map.of("partitioner.strategy", "TIME_BASED");

        // When
        PartitionStrategy strategy = PartitionStrategyFactory.createStrategy(config);

        // Then
        assertThat(strategy).isInstanceOf(TimeBasedPartitionStrategy.class);
        assertThat(strategy.getStrategyName()).isEqualTo("TIME_BASED");
    }

    @Test
    void partitionStrategyFactory_shouldCreateTopicPartitionStrategy() {
        // Given
        Map<String, String> config = Map.of("partitioner.strategy", "TOPIC_PARTITION");

        // When
        PartitionStrategy strategy = PartitionStrategyFactory.createStrategy(config);

        // Then
        assertThat(strategy).isInstanceOf(TopicPartitionStrategy.class);
        assertThat(strategy.getStrategyName()).isEqualTo("TOPIC_PARTITION");
    }

    @Test
    void partitionStrategyFactory_shouldDefaultForUnknownStrategy() {
        // Given
        Map<String, String> config = Map.of("partitioner.strategy", "UNKNOWN");

        // When
        PartitionStrategy strategy = PartitionStrategyFactory.createStrategy(config);

        // Then - should default to DEFAULT strategy
        assertThat(strategy).isInstanceOf(DefaultPartitionStrategy.class);
        assertThat(strategy.getStrategyName()).isEqualTo("DEFAULT");
    }

    @Test
    void partitionStrategyFactory_shouldDefaultToDefaultStrategy() {
        // Given
        Map<String, String> config = Map.of(); // No strategy specified

        // When
        PartitionStrategy strategy = PartitionStrategyFactory.createStrategy(config);

        // Then
        assertThat(strategy).isInstanceOf(DefaultPartitionStrategy.class);
        assertThat(strategy.getStrategyName()).isEqualTo("DEFAULT");
    }

    private SinkRecord createSinkRecord(String topic, int partition, String key, String value) {
        return new SinkRecord(topic, partition, null, key, null, value, 0);
    }

    private SinkRecord createSinkRecordWithTimestamp(String topic, int partition, String key, String value, long timestamp) {
        return new SinkRecord(topic, partition, null, key, null, value, 0, timestamp, null);
    }
}