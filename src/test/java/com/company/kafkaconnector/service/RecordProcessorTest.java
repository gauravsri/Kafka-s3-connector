package com.company.kafkaconnector.service;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class RecordProcessorTest {

    private RecordProcessor recordProcessor;

    @BeforeEach
    void setUp() {
        recordProcessor = new RecordProcessor();
    }

    @Test
    void shouldProcessRecordWithMapValue() {
        // Given
        Map<String, Object> originalValue = new HashMap<>();
        originalValue.put("user_id", "123");
        originalValue.put("event_type", "login");

        SinkRecord record = new SinkRecord(
            "test-topic", 0, null, "test-key", null, originalValue, 100L, 1234567890L, null
        );

        // When
        SinkRecord processedRecord = recordProcessor.process(record);

        // Then
        assertThat(processedRecord).isNotNull();
        assertThat(processedRecord.value()).isInstanceOf(Map.class);
        
        Map<String, Object> processedValue = (Map<String, Object>) processedRecord.value();
        assertThat(processedValue.get("user_id")).isEqualTo("123");
        assertThat(processedValue.get("event_type")).isEqualTo("login");
        assertThat(processedValue.get("_metadata")).isNotNull();
        
        Map<String, Object> metadata = (Map<String, Object>) processedValue.get("_metadata");
        assertThat(metadata.get("kafka_topic")).isEqualTo("test-topic");
        assertThat(metadata.get("kafka_partition")).isEqualTo(0);
        assertThat(metadata.get("kafka_offset")).isEqualTo(100L);
        assertThat(metadata.get("kafka_timestamp")).isEqualTo(1234567890L);
        assertThat(metadata.get("processor_version")).isEqualTo("1.0.0");
        assertThat(metadata.get("processing_timestamp")).isNotNull();
    }

    @Test
    void shouldProcessRecordWithStructValue() {
        // Given
        Schema schema = SchemaBuilder.struct()
            .name("TestSchema")
            .field("user_id", Schema.STRING_SCHEMA)
            .field("event_type", Schema.STRING_SCHEMA)
            .build();

        Struct originalValue = new Struct(schema)
            .put("user_id", "456")
            .put("event_type", "logout");

        SinkRecord record = new SinkRecord(
            "test-topic", 1, null, "test-key", schema, originalValue, 200L, 9876543210L, null
        );

        // When
        SinkRecord processedRecord = recordProcessor.process(record);

        // Then
        assertThat(processedRecord).isNotNull();
        assertThat(processedRecord.value()).isInstanceOf(Map.class);
        
        Map<String, Object> processedValue = (Map<String, Object>) processedRecord.value();
        assertThat(processedValue.get("user_id")).isEqualTo("456");
        assertThat(processedValue.get("event_type")).isEqualTo("logout");
        assertThat(processedValue.get("_metadata")).isNotNull();
    }

    @Test
    void shouldProcessRecordWithStringValue() {
        // Given
        SinkRecord record = new SinkRecord(
            "test-topic", 2, null, "test-key", Schema.STRING_SCHEMA, "simple string value", 300L, 1111111111L, null
        );

        // When
        SinkRecord processedRecord = recordProcessor.process(record);

        // Then
        assertThat(processedRecord).isNotNull();
        assertThat(processedRecord.value()).isInstanceOf(Map.class);
        
        Map<String, Object> processedValue = (Map<String, Object>) processedRecord.value();
        assertThat(processedValue.get("original_value")).isEqualTo("simple string value");
        assertThat(processedValue.get("_metadata")).isNotNull();
    }

    @Test
    void shouldProcessRecordWithNullValue() {
        // Given
        SinkRecord record = new SinkRecord(
            "test-topic", 0, null, "test-key", null, null, 400L, 2222222222L, null
        );

        // When
        SinkRecord processedRecord = recordProcessor.process(record);

        // Then
        assertThat(processedRecord).isNotNull();
        assertThat(processedRecord.value()).isInstanceOf(Map.class);
        
        Map<String, Object> processedValue = (Map<String, Object>) processedRecord.value();
        assertThat(processedValue.get("_metadata")).isNotNull();
        
        Map<String, Object> metadata = (Map<String, Object>) processedValue.get("_metadata");
        assertThat(metadata.get("kafka_topic")).isEqualTo("test-topic");
        assertThat(metadata.get("kafka_offset")).isEqualTo(400L);
    }

    @Test
    void shouldPreserveOriginalRecordMetadata() {
        // Given
        Map<String, Object> originalValue = new HashMap<>();
        originalValue.put("data", "test");

        SinkRecord record = new SinkRecord(
            "my-topic", 5, Schema.STRING_SCHEMA, "my-key", null, originalValue, 999L, 3333333333L, null
        );

        // When
        SinkRecord processedRecord = recordProcessor.process(record);

        // Then
        assertThat(processedRecord.topic()).isEqualTo("my-topic");
        assertThat(processedRecord.kafkaPartition()).isEqualTo(5);
        assertThat(processedRecord.key()).isEqualTo("my-key");
        assertThat(processedRecord.keySchema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(processedRecord.kafkaOffset()).isEqualTo(999L);
        assertThat(processedRecord.timestamp()).isEqualTo(3333333333L);
    }

    @Test
    void shouldHandleEmptyMap() {
        // Given
        Map<String, Object> emptyMap = new HashMap<>();
        SinkRecord record = new SinkRecord(
            "test-topic", 0, null, "test-key", null, emptyMap, 500L, 4444444444L, null
        );

        // When
        SinkRecord processedRecord = recordProcessor.process(record);

        // Then
        assertThat(processedRecord).isNotNull();
        Map<String, Object> processedValue = (Map<String, Object>) processedRecord.value();
        assertThat(processedValue.get("_metadata")).isNotNull();
        assertThat(processedValue.size()).isEqualTo(1); // Only metadata
    }

    @Test
    void shouldHandleStructWithNullFields() {
        // Given
        Schema schema = SchemaBuilder.struct()
            .name("TestSchema")
            .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
            .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

        Struct originalValue = new Struct(schema)
            .put("field1", "value1")
            .put("field2", null);

        SinkRecord record = new SinkRecord(
            "test-topic", 0, null, "test-key", schema, originalValue, 600L, 5555555555L, null
        );

        // When
        SinkRecord processedRecord = recordProcessor.process(record);

        // Then
        assertThat(processedRecord).isNotNull();
        Map<String, Object> processedValue = (Map<String, Object>) processedRecord.value();
        assertThat(processedValue.get("field1")).isEqualTo("value1");
        assertThat(processedValue.containsKey("field2")).isFalse(); // Null fields are not included
        assertThat(processedValue.get("_metadata")).isNotNull();
    }

    @Test
    void shouldCreateEnhancedSchema() {
        // Given
        Schema originalSchema = SchemaBuilder.struct()
            .name("OriginalSchema")
            .version(1)
            .field("field1", Schema.STRING_SCHEMA)
            .build();

        Struct originalValue = new Struct(originalSchema)
            .put("field1", "test");

        SinkRecord record = new SinkRecord(
            "test-topic", 0, null, "test-key", originalSchema, originalValue, 700L, 6666666666L, null
        );

        // When
        SinkRecord processedRecord = recordProcessor.process(record);

        // Then
        assertThat(processedRecord.valueSchema()).isNotNull();
        assertThat(processedRecord.valueSchema().name()).contains("enhanced");
        assertThat(processedRecord.valueSchema().fields()).isNotEmpty();
    }
}