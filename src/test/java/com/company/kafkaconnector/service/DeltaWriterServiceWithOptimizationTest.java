package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.DeltaWriteException;
import com.company.kafkaconnector.model.DeltaConfig;
import com.company.kafkaconnector.model.ProcessingConfig;
import com.company.kafkaconnector.model.TopicConfig;
import com.company.kafkaconnector.model.DestinationConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeltaWriterServiceWithOptimizationTest {

    @Mock
    private DeltaKernelWriterService mockDeltaKernelWriterService;

    @Mock
    private DeltaOptimizationService mockDeltaOptimizationService;

    private DeltaWriterService deltaWriterService;
    private TopicConfig testTopicConfig;

    @BeforeEach
    void setUp() {
        deltaWriterService = new DeltaWriterService(mockDeltaKernelWriterService, mockDeltaOptimizationService);
        
        // Setup test topic configuration
        DeltaConfig deltaConfig = new DeltaConfig(true, 5, false, 168, true, "10");
        
        DestinationConfig destination = new DestinationConfig();
        destination.setBucket("test-bucket");
        destination.setPath("test/path");
        destination.setTableName("test_table");
        destination.setPartitionColumns(List.of("year", "month", "day"));
        destination.setDeltaConfig(deltaConfig);
        
        ProcessingConfig processing = new ProcessingConfig();
        processing.setBatchSize(3);
        processing.setFlushInterval(60);
        processing.setMaxRetries(3);
        
        testTopicConfig = new TopicConfig();
        testTopicConfig.setKafkaTopic("test.topic.v1");
        testTopicConfig.setDestination(destination);
        testTopicConfig.setProcessing(processing);
    }

    @Test
    void shouldTrackBatchWriteAfterSuccessfulFlush() throws IOException {
        // Given
        Map<String, Object> message1 = createTestMessage("user1");
        Map<String, Object> message2 = createTestMessage("user2");
        Map<String, Object> message3 = createTestMessage("user3");

        // When - write messages to trigger batch flush
        deltaWriterService.writeMessage(message1, testTopicConfig);
        deltaWriterService.writeMessage(message2, testTopicConfig);
        deltaWriterService.writeMessage(message3, testTopicConfig);

        // Then
        verify(mockDeltaKernelWriterService, times(1)).write(eq(testTopicConfig), any(), any());
        verify(mockDeltaOptimizationService, times(1)).trackBatchWrite("s3a://test-bucket/test/path");
        verify(mockDeltaOptimizationService, times(1)).performOptimizations(testTopicConfig);
    }

    @Test
    void shouldStoreTopicConfigForFlushAll() {
        // Given
        Map<String, Object> message = createTestMessage("user1");

        // When
        deltaWriterService.writeMessage(message, testTopicConfig);

        // Then - verify topic config is stored (will be used in flushAllBatches test)
        // We can't directly verify the private map, but we can test flushAllBatches behavior
        assertDoesNotThrow(() -> deltaWriterService.flushAllBatches());
    }

    @Test
    void shouldCallOptimizationAfterEachBatch() throws IOException {
        // Given
        Map<String, Object> message1 = createTestMessage("user1");
        Map<String, Object> message2 = createTestMessage("user2");
        Map<String, Object> message3 = createTestMessage("user3");

        // When
        deltaWriterService.writeMessage(message1, testTopicConfig);
        deltaWriterService.writeMessage(message2, testTopicConfig);
        deltaWriterService.writeMessage(message3, testTopicConfig); // Should trigger flush

        // Then
        verify(mockDeltaOptimizationService, times(1)).performOptimizations(testTopicConfig);
    }

    @Test
    void shouldHandleOptimizationErrors() throws IOException {
        // Given
        Map<String, Object> message1 = createTestMessage("user1");
        Map<String, Object> message2 = createTestMessage("user2");
        Map<String, Object> message3 = createTestMessage("user3");
        
        // Mock optimization to throw an exception
        doThrow(new RuntimeException("Optimization failed")).when(mockDeltaOptimizationService)
            .performOptimizations(testTopicConfig);

        // When/Then - should not propagate optimization errors
        assertDoesNotThrow(() -> {
            deltaWriterService.writeMessage(message1, testTopicConfig);
            deltaWriterService.writeMessage(message2, testTopicConfig);
            deltaWriterService.writeMessage(message3, testTopicConfig);
        });

        // Verify the write still happened despite optimization failure
        verify(mockDeltaKernelWriterService, times(1)).write(eq(testTopicConfig), any(), any());
    }

    @Test
    void shouldFlushAllBatchesWithStoredConfigs() throws IOException {
        // Given - add messages to multiple topics/destinations
        TopicConfig anotherTopicConfig = createAnotherTopicConfig();
        
        Map<String, Object> message1 = createTestMessage("user1");
        Map<String, Object> message2 = createTestMessage("user2");
        Map<String, Object> messageOther = createTestMessage("userOther");

        // Add messages but don't reach batch size to avoid auto-flush
        deltaWriterService.writeMessage(message1, testTopicConfig);
        deltaWriterService.writeMessage(message2, testTopicConfig);
        deltaWriterService.writeMessage(messageOther, anotherTopicConfig);

        // When
        deltaWriterService.flushAllBatches();

        // Then - should flush both batches
        verify(mockDeltaKernelWriterService, times(2)).write(any(), any(), any());
        verify(mockDeltaOptimizationService, times(2)).trackBatchWrite(anyString());
        verify(mockDeltaOptimizationService, times(2)).performOptimizations(any());
    }

    @Test
    void shouldHandleFlushAllBatchesErrors() throws IOException {
        // Given
        Map<String, Object> message = createTestMessage("user1");
        deltaWriterService.writeMessage(message, testTopicConfig);
        
        // Mock write to throw an exception
        doThrow(new IOException("Write failed")).when(mockDeltaKernelWriterService)
            .write(any(), any(), any());

        // When/Then - should not throw exception
        assertDoesNotThrow(() -> deltaWriterService.flushAllBatches());
    }

    @Test
    void shouldPropagateWriteErrors() throws IOException {
        // Given
        Map<String, Object> message1 = createTestMessage("user1");
        Map<String, Object> message2 = createTestMessage("user2");
        Map<String, Object> message3 = createTestMessage("user3");
        
        doThrow(new IOException("Write failed")).when(mockDeltaKernelWriterService)
            .write(any(), any(), any());

        // When/Then
        deltaWriterService.writeMessage(message1, testTopicConfig);
        deltaWriterService.writeMessage(message2, testTopicConfig);
        
        assertThrows(DeltaWriteException.class, () -> {
            deltaWriterService.writeMessage(message3, testTopicConfig); // Should trigger flush and fail
        });
    }

    @Test
    void shouldNotCallOptimizationForPartialBatches() {
        // Given
        Map<String, Object> message = createTestMessage("user1");

        // When - write only one message (below batch size)
        deltaWriterService.writeMessage(message, testTopicConfig);

        // Then - should not call optimization
        verify(mockDeltaOptimizationService, never()).performOptimizations(any());
        verify(mockDeltaOptimizationService, never()).trackBatchWrite(any());
    }

    @Test
    void shouldHandleEmptyBatchesInFlushAll() throws IOException {
        // Given - no messages added
        
        // When
        deltaWriterService.flushAllBatches();

        // Then - should complete without errors
        verify(mockDeltaKernelWriterService, never()).write(any(), any(), any());
    }

    @Test
    void shouldTrackMultipleTablesPaths() throws IOException {
        // Given
        TopicConfig topic1 = testTopicConfig;
        TopicConfig topic2 = createAnotherTopicConfig();
        
        // Create enough messages to flush both topics
        Map<String, Object> message1a = createTestMessage("user1a");
        Map<String, Object> message1b = createTestMessage("user1b");
        Map<String, Object> message1c = createTestMessage("user1c");
        
        Map<String, Object> message2a = createTestMessage("user2a");
        Map<String, Object> message2b = createTestMessage("user2b");
        Map<String, Object> message2c = createTestMessage("user2c");

        // When
        deltaWriterService.writeMessage(message1a, topic1);
        deltaWriterService.writeMessage(message1b, topic1);
        deltaWriterService.writeMessage(message1c, topic1); // Flush topic1
        
        deltaWriterService.writeMessage(message2a, topic2);
        deltaWriterService.writeMessage(message2b, topic2);
        deltaWriterService.writeMessage(message2c, topic2); // Flush topic2

        // Then
        verify(mockDeltaOptimizationService, times(1)).trackBatchWrite("s3a://test-bucket/test/path");
        verify(mockDeltaOptimizationService, times(1)).trackBatchWrite("s3a://another-bucket/another/path");
        verify(mockDeltaOptimizationService, times(2)).performOptimizations(any());
    }

    private Map<String, Object> createTestMessage(String userId) {
        Map<String, Object> message = new HashMap<>();
        message.put("user_id", userId);
        message.put("event_type", "click");
        message.put("timestamp", System.currentTimeMillis());
        return message;
    }

    private TopicConfig createAnotherTopicConfig() {
        DeltaConfig deltaConfig = new DeltaConfig(true, 3, true, 72, true, "5");
        
        DestinationConfig destination = new DestinationConfig();
        destination.setBucket("another-bucket");
        destination.setPath("another/path");
        destination.setTableName("another_table");
        destination.setPartitionColumns(List.of("year", "month"));
        destination.setDeltaConfig(deltaConfig);
        
        ProcessingConfig processing = new ProcessingConfig();
        processing.setBatchSize(3);
        processing.setFlushInterval(30);
        processing.setMaxRetries(5);
        
        TopicConfig config = new TopicConfig();
        config.setKafkaTopic("another.topic.v1");
        config.setDestination(destination);
        config.setProcessing(processing);
        
        return config;
    }
}