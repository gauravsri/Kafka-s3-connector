package com.company.kafkaconnector.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for ConnectorMetrics
 */
class ConnectorMetricsTest {

    private MeterRegistry meterRegistry;
    private ConnectorMetrics connectorMetrics;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        connectorMetrics = new ConnectorMetrics(meterRegistry);
    }

    @Test
    void shouldIncrementRecordProcessedCounter() {
        // Given
        String topic = "user-events";

        // When
        connectorMetrics.incrementRecordsProcessed(topic);
        connectorMetrics.incrementRecordsProcessed(topic);

        // Then
        Counter counter = meterRegistry.find("kafka_connector_records_processed_total")
                .counter();
        
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(2.0);
    }

    @Test
    void shouldIncrementRecordsFailedCounter() {
        // Given
        String topic = "order-events";

        // When
        connectorMetrics.incrementRecordsFailed(topic);

        // Then
        Counter counter = meterRegistry.find("kafka_connector_records_failed_total")
                .counter();
        
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldRecordS3WriteTime() {
        // Given
        String topic = "user-events";

        // When
        Timer.Sample sample = connectorMetrics.startS3WriteTimer();
        // Simulate some processing time
        try { Thread.sleep(1); } catch (InterruptedException e) {}
        connectorMetrics.recordS3WriteTime(sample);

        // Then
        Timer timer = meterRegistry.find("kafka_connector_s3_write_duration")
                .timer();
        
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                .isGreaterThan(0);
    }

    @Test
    void shouldIncrementSchemaValidationFailures() {
        // Given
        // When
        connectorMetrics.incrementSchemaValidationFailures();
        connectorMetrics.incrementSchemaValidationFailures();
        connectorMetrics.incrementSchemaValidationFailures();

        // Then
        Counter counter = meterRegistry.find("kafka_connector_schema_validation_failures_total")
                .counter();
        
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(3.0);
    }

    @Test
    void shouldIncrementDeadLetterQueueMessages() {
        // Given
        // When
        connectorMetrics.incrementDlqMessages();

        // Then
        Counter counter = meterRegistry.find("kafka_connector_dlq_messages_total")
                .counter();
        
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldTrackActiveConnections() {
        // Given
        long connectionCount = 4;

        // When
        connectorMetrics.setActiveConnections(connectionCount);

        // Then
        Gauge gauge = meterRegistry.find("kafka_connector_active_connections").gauge();
        
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(4.0);
    }

    @Test
    void shouldTrackMultipleTopics() {
        // Given
        String topic1 = "user-events";
        String topic2 = "order-events";

        // When
        connectorMetrics.incrementRecordsProcessed(topic1);
        connectorMetrics.incrementRecordsProcessed(topic1);
        connectorMetrics.incrementRecordsProcessed(topic2);

        // Then - main counter should track all records
        Counter mainCounter = meterRegistry.find("kafka_connector_records_processed_total")
                .counter();
        
        assertThat(mainCounter.count()).isEqualTo(3.0);
        
        // Topic-specific metrics should be tracked separately
        assertThat(connectorMetrics.getTopicMetrics(topic1, false)).isNotNull();
        assertThat(connectorMetrics.getTopicMetrics(topic2, false)).isNotNull();
    }

    @Test
    void shouldCreateMetricsSnapshot() {
        // Given
        connectorMetrics.incrementRecordsProcessed("user-events");
        connectorMetrics.incrementRecordsProcessed("user-events");
        connectorMetrics.incrementRecordsFailed("user-events");
        Timer.Sample sample = connectorMetrics.startS3WriteTimer();
        connectorMetrics.recordS3WriteTime(sample);
        connectorMetrics.setActiveConnections(2);

        // When
        ConnectorMetricsSnapshot snapshot = connectorMetrics.getSnapshot();

        // Then
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.getRecordsProcessed()).isEqualTo(2.0);
        assertThat(snapshot.getRecordsFailed()).isEqualTo(1.0);
        assertThat(snapshot.getActiveConnections()).isEqualTo(2);
        assertThat(snapshot.getAvgS3WriteTime()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void shouldHandleNullTopic() {
        // When & Then - should not throw exception for basic operations
        assertThatNoException().isThrownBy(() -> {
            connectorMetrics.incrementRecordsProcessed();
            connectorMetrics.incrementRecordsFailed();
            connectorMetrics.incrementS3Writes();
        });
    }
}