package com.company.kafkaconnector.service;

import com.company.kafkaconnector.model.DestinationConfig;
import com.company.kafkaconnector.model.EventMessage;
import com.company.kafkaconnector.model.TopicConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class MessageTransformationServiceTest {

    private MessageTransformationService messageTransformationService;
    private EventMessage eventMessage;
    private TopicConfig topicConfig;

    @BeforeEach
    void setUp() {
        messageTransformationService = new MessageTransformationService();
        
        eventMessage = new EventMessage();
        eventMessage.setTopicName("test-topic");
        eventMessage.setPartition("1");
        eventMessage.setOffset(100L);
        eventMessage.setKey("test-key");
        eventMessage.setValue("{\"user_id\": \"123\", \"event_type\": \"login\", \"timestamp\": \"2023-01-01T10:00:00Z\"}");
        eventMessage.setProcessedAt(LocalDateTime.of(2023, 1, 1, 10, 0));

        topicConfig = new TopicConfig();
        DestinationConfig destinationConfig = new DestinationConfig();
        destinationConfig.setPartitionColumns(Arrays.asList("year", "month"));
        topicConfig.setDestination(destinationConfig);
    }

    @Test
    void shouldTransformMessageWithMetadata() {
        Map<String, Object> result = messageTransformationService.transform(eventMessage, topicConfig);

        assertThat(result).isNotNull();
        assertThat(result.get("user_id")).isEqualTo("123");
        assertThat(result.get("event_type")).isEqualTo("login");
        assertThat(result.get("timestamp")).isEqualTo("2023-01-01T10:00:00Z");
        
        assertThat(result.get("_kafka_topic")).isEqualTo("test-topic");
        assertThat(result.get("_kafka_partition")).isEqualTo("1");
        assertThat(result.get("_kafka_offset")).isEqualTo(100L);
        assertThat(result.get("_kafka_key")).isEqualTo("test-key");
        assertThat(result.get("_processed_at")).isEqualTo("2023-01-01T10:00");
        assertThat(result.get("_ingestion_timestamp")).isNotNull();
    }

    @Test
    void shouldAddPartitionColumns() {
        Map<String, Object> result = messageTransformationService.transform(eventMessage, topicConfig);

        assertThat(result.get("year")).isNotNull();
        assertThat(result.get("month")).isNotNull();
        assertThat(result.get("year")).asString().matches("\\d{4}");
        assertThat(result.get("month")).asString().matches("\\d{2}");
    }

    @Test
    void shouldHandleAllPartitionColumnTypes() {
        DestinationConfig destinationConfig = new DestinationConfig();
        destinationConfig.setPartitionColumns(Arrays.asList("year", "month", "day", "hour"));
        topicConfig.setDestination(destinationConfig);

        Map<String, Object> result = messageTransformationService.transform(eventMessage, topicConfig);

        assertThat(result.get("year")).isNotNull();
        assertThat(result.get("month")).isNotNull();
        assertThat(result.get("day")).isNotNull();
        assertThat(result.get("hour")).isNotNull();
    }

    @Test
    void shouldHandleCustomPartitionColumns() {
        DestinationConfig destinationConfig = new DestinationConfig();
        destinationConfig.setPartitionColumns(Arrays.asList("region", "environment"));
        topicConfig.setDestination(destinationConfig);

        Map<String, Object> result = messageTransformationService.transform(eventMessage, topicConfig);

        assertThat(result.get("region")).isEqualTo("unknown");
        assertThat(result.get("environment")).isEqualTo("unknown");
    }

    @Test
    void shouldPreserveExistingCustomPartitionColumns() {
        eventMessage.setValue("{\"user_id\": \"123\", \"region\": \"us-west\", \"environment\": \"prod\"}");
        
        DestinationConfig destinationConfig = new DestinationConfig();
        destinationConfig.setPartitionColumns(Arrays.asList("region", "environment"));
        topicConfig.setDestination(destinationConfig);

        Map<String, Object> result = messageTransformationService.transform(eventMessage, topicConfig);

        assertThat(result.get("region")).isEqualTo("us-west");
        assertThat(result.get("environment")).isEqualTo("prod");
    }

    @Test
    void shouldThrowExceptionForInvalidJson() {
        eventMessage.setValue("invalid json");

        assertThatThrownBy(() -> messageTransformationService.transform(eventMessage, topicConfig))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Message transformation failed");
    }

    @Test
    void shouldHandleEmptyPartitionColumns() {
        DestinationConfig destinationConfig = new DestinationConfig();
        destinationConfig.setPartitionColumns(Arrays.asList());
        topicConfig.setDestination(destinationConfig);

        Map<String, Object> result = messageTransformationService.transform(eventMessage, topicConfig);

        assertThat(result).isNotNull();
        assertThat(result.get("user_id")).isEqualTo("123");
        assertThat(result.get("_kafka_topic")).isEqualTo("test-topic");
    }

    @Test
    void shouldHandleNullValues() {
        eventMessage.setValue("{\"user_id\": null, \"event_type\": \"login\"}");

        Map<String, Object> result = messageTransformationService.transform(eventMessage, topicConfig);

        assertThat(result).isNotNull();
        assertThat(result.get("user_id")).isNull();
        assertThat(result.get("event_type")).isEqualTo("login");
    }

    @Test
    void shouldHandleNestedJsonObjects() {
        eventMessage.setValue("{\"user\": {\"id\": \"123\", \"name\": \"John\"}, \"event_type\": \"login\"}");

        Map<String, Object> result = messageTransformationService.transform(eventMessage, topicConfig);

        assertThat(result).isNotNull();
        assertThat(result.get("user")).isInstanceOf(Map.class);
        assertThat(result.get("event_type")).isEqualTo("login");
    }
}