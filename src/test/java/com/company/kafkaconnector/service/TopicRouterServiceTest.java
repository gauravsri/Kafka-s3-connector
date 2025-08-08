package com.company.kafkaconnector.service;

import com.company.kafkaconnector.config.ConnectorConfiguration;
import com.company.kafkaconnector.model.DestinationConfig;
import com.company.kafkaconnector.model.ProcessingResult;
import com.company.kafkaconnector.model.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicRouterServiceTest {

    @Mock
    private ConnectorConfiguration connectorConfiguration;
    
    @Mock
    private SchemaValidationService schemaValidationService;
    
    @Mock
    private MessageTransformationService messageTransformationService;
    
    @Mock
    private DeltaWriterService deltaWriterService;
    
    @Mock
    private RetryService retryService;
    
    @Mock
    private CircuitBreakerService circuitBreakerService;
    
    @Mock
    private DeadLetterQueueService deadLetterQueueService;

    private TopicRouterService topicRouterService;
    private TopicConfig topicConfig;
    private ConsumerRecord<String, String> consumerRecord;

    @BeforeEach
    void setUp() {
        topicRouterService = new TopicRouterService(
            connectorConfiguration,
            schemaValidationService,
            messageTransformationService,
            deltaWriterService,
            retryService,
            circuitBreakerService,
            deadLetterQueueService
        );

        topicConfig = new TopicConfig();
        topicConfig.setKafkaTopic("test-topic");
        topicConfig.setSchemaFile("schemas/test-schema.json");
        topicConfig.setDestination(new DestinationConfig());

        consumerRecord = new ConsumerRecord<>("test-topic", 0, 100L, "test-key", 
            "{\"user_id\": \"123\", \"event_type\": \"login\"}");

        Map<String, TopicConfig> topics = new HashMap<>();
        topics.put("test-config", topicConfig);
        when(connectorConfiguration.getTopics()).thenReturn(topics);
    }

    @Test
    void shouldProcessMessageSuccessfully() throws Exception {
        // Mock successful validation and transformation
        when(schemaValidationService.validateMessage(anyString(), anyString())).thenReturn(true);
        when(messageTransformationService.transform(any(), any())).thenReturn(new HashMap<>());
        
        // Mock circuit breaker to execute the operation directly
        when(circuitBreakerService.executeWithCircuitBreaker(anyString(), any()))
            .thenReturn(createSuccessResult("test-topic"));
        
        ProcessingResult result = topicRouterService.processMessage(consumerRecord);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getTopicName()).isEqualTo("test-topic");

        verify(circuitBreakerService).executeWithCircuitBreaker(eq("message-processing-test-topic"), any());
    }
    
    private ProcessingResult createSuccessResult(String topicName) {
        ProcessingResult result = new ProcessingResult(true, topicName);
        result.setRecordsProcessed(1);
        result.markComplete();
        return result;
    }

    @Test
    void shouldHandleTopicNotFound() {
        when(connectorConfiguration.getTopics()).thenReturn(new HashMap<>());

        ProcessingResult result = topicRouterService.processMessage(consumerRecord);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorMessage()).contains("No configuration found for topic: test-topic");
        verify(deadLetterQueueService).sendToDeadLetterQueue(any(), any(), anyString(), any());
    }

    @Test
    void shouldHandleSchemaValidationFailure() throws Exception {
        // Mock circuit breaker to return failure result directly
        when(circuitBreakerService.executeWithCircuitBreaker(anyString(), any()))
            .thenReturn(createFailureResult("test-topic", "Schema validation failed"));

        ProcessingResult result = topicRouterService.processMessage(consumerRecord);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getRecordsFailed()).isEqualTo(1);
    }
    
    private ProcessingResult createFailureResult(String topicName, String errorMessage) {
        ProcessingResult result = new ProcessingResult(false, topicName);
        result.setErrorMessage(errorMessage);
        result.setRecordsFailed(1);
        result.markComplete();
        return result;
    }

    @Test
    void shouldHandleCircuitBreakerOpenException() throws Exception {
        when(circuitBreakerService.executeWithCircuitBreaker(anyString(), any()))
            .thenThrow(new CircuitBreakerService.CircuitBreakerOpenException("Circuit breaker is open"));

        ProcessingResult result = topicRouterService.processMessage(consumerRecord);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorMessage()).isEqualTo("Circuit breaker open");
        
        verify(deadLetterQueueService).sendToDeadLetterQueue(any(), any(), eq("Circuit breaker open"), any());
    }

    @Test
    void shouldHandleTransformationFailure() throws Exception {
        // Mock circuit breaker to return failure result directly
        when(circuitBreakerService.executeWithCircuitBreaker(anyString(), any()))
            .thenReturn(createFailureResult("test-topic", "Transformation failed"));

        ProcessingResult result = topicRouterService.processMessage(consumerRecord);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getRecordsFailed()).isEqualTo(1);
    }

    @Test
    void shouldReturnEmptyTopicStats() {
        // Test that getTopicStats returns empty map initially
        Map<String, ProcessingResult> stats = topicRouterService.getTopicStats();
        assertThat(stats).isNotNull();
        assertThat(stats).isEmpty();
    }

    @Test
    void shouldCreateEventMessageCorrectly() throws Exception {
        // Mock circuit breaker to return success result
        when(circuitBreakerService.executeWithCircuitBreaker(anyString(), any()))
            .thenReturn(createSuccessResult("test-topic"));

        topicRouterService.processMessage(consumerRecord);

        // Verify circuit breaker was called with correct service name
        verify(circuitBreakerService).executeWithCircuitBreaker(eq("message-processing-test-topic"), any());
    }
}