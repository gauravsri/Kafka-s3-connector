package com.company.kafkaconnector.service;

import com.company.kafkaconnector.model.ProcessingConfig;
import com.company.kafkaconnector.model.TopicConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.*;

class RetryServiceTest {

    private RetryService retryService;
    private TopicConfig topicConfig;

    @BeforeEach
    void setUp() {
        retryService = new RetryService();
        
        topicConfig = new TopicConfig();
        ProcessingConfig processingConfig = new ProcessingConfig();
        processingConfig.setMaxRetries(3);
        topicConfig.setProcessing(processingConfig);
    }

    @Test
    void shouldSucceedOnFirstAttempt() {
        // Given
        Supplier<String> operation = () -> "success";

        // When
        String result = retryService.executeWithRetry("test-topic", topicConfig, operation, "test-operation");

        // Then
        assertThat(result).isEqualTo("success");
        
        RetryService.RetryStats stats = retryService.getRetryStats("test-topic");
        assertThat(stats.getCurrentAttempt()).isEqualTo(0);
        assertThat(stats.getTotalFailures()).isEqualTo(0);
    }

    @Test
    void shouldRetryAndSucceedOnSecondAttempt() {
        // Given
        AtomicInteger attempts = new AtomicInteger(0);
        Supplier<String> operation = () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("First attempt failed");
            }
            return "success on retry";
        };

        // When
        String result = retryService.executeWithRetry("test-topic", topicConfig, operation, "test-operation");

        // Then
        assertThat(result).isEqualTo("success on retry");
        assertThat(attempts.get()).isEqualTo(2);
        
        // After success, retry state should be reset
        RetryService.RetryStats stats = retryService.getRetryStats("test-topic");
        assertThat(stats.getCurrentAttempt()).isEqualTo(0);
    }

    @Test
    void shouldFailAfterMaxRetries() {
        // Given
        AtomicInteger attempts = new AtomicInteger(0);
        Supplier<String> operation = () -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Always fails");
        };

        // When/Then
        assertThatThrownBy(() -> 
            retryService.executeWithRetry("test-topic", topicConfig, operation, "test-operation"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Operation failed after 4 attempts");

        // Should have attempted 4 times (1 initial + 3 retries)
        assertThat(attempts.get()).isEqualTo(4);
        
        RetryService.RetryStats stats = retryService.getRetryStats("test-topic");
        assertThat(stats.getTotalFailures()).isEqualTo(1);
        assertThat(stats.getLastError()).contains("Always fails");
        assertThat(stats.getLastFailureTime()).isNotNull();
    }

    @Test
    void shouldHandleZeroMaxRetries() {
        // Given
        ProcessingConfig processingConfig = new ProcessingConfig();
        processingConfig.setMaxRetries(0);
        topicConfig.setProcessing(processingConfig);
        
        AtomicInteger attempts = new AtomicInteger(0);
        Supplier<String> operation = () -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Fails immediately");
        };

        // When/Then
        assertThatThrownBy(() -> 
            retryService.executeWithRetry("test-topic", topicConfig, operation, "test-operation"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Operation failed after 1 attempts");

        // Should have attempted only once
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void shouldReturnEmptyStatsForUnknownTopic() {
        // When
        RetryService.RetryStats stats = retryService.getRetryStats("unknown-topic");

        // Then
        assertThat(stats.getTopicName()).isEqualTo("unknown-topic");
        assertThat(stats.getCurrentAttempt()).isEqualTo(0);
        assertThat(stats.getTotalFailures()).isEqualTo(0);
        assertThat(stats.getLastError()).isNull();
        assertThat(stats.getLastFailureTime()).isNull();
    }

    @Test
    void shouldTrackMultipleTopicStats() {
        // Given
        Supplier<String> failingOperation = () -> {
            throw new RuntimeException("Failing operation");
        };

        // When
        assertThatThrownBy(() -> 
            retryService.executeWithRetry("topic1", topicConfig, failingOperation, "test-operation"));
        assertThatThrownBy(() -> 
            retryService.executeWithRetry("topic2", topicConfig, failingOperation, "test-operation"));

        // Then
        RetryService.RetryStats stats1 = retryService.getRetryStats("topic1");
        RetryService.RetryStats stats2 = retryService.getRetryStats("topic2");
        
        assertThat(stats1.getTopicName()).isEqualTo("topic1");
        assertThat(stats2.getTopicName()).isEqualTo("topic2");
        assertThat(stats1.getTotalFailures()).isEqualTo(1);
        assertThat(stats2.getTotalFailures()).isEqualTo(1);
    }

    @Test
    void shouldHandleInterruptedException() throws InterruptedException {
        // Given
        AtomicInteger attempts = new AtomicInteger(0);
        Supplier<String> operation = () -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Always fails");
        };

        // Interrupt the current thread before retry
        Thread.currentThread().interrupt();

        // When/Then
        assertThatThrownBy(() -> 
            retryService.executeWithRetry("test-topic", topicConfig, operation, "test-operation"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Retry interrupted");

        // Should have attempted only once before interruption
        assertThat(attempts.get()).isEqualTo(1);
        
        // Clear interrupt flag for other tests
        Thread.interrupted();
    }

    @Test
    void shouldSucceedAfterMultipleFailures() {
        // Given
        AtomicInteger attempts = new AtomicInteger(0);
        Supplier<String> operation = () -> {
            if (attempts.incrementAndGet() <= 2) {
                throw new RuntimeException("Attempt " + attempts.get() + " failed");
            }
            return "success after retries";
        };

        // When
        String result = retryService.executeWithRetry("test-topic", topicConfig, operation, "test-operation");

        // Then
        assertThat(result).isEqualTo("success after retries");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void shouldIncrementTotalFailuresAcrossOperations() {
        // Given
        Supplier<String> failingOperation = () -> {
            throw new RuntimeException("Always fails");
        };

        // When - multiple failed operations
        assertThatThrownBy(() -> 
            retryService.executeWithRetry("test-topic", topicConfig, failingOperation, "test-operation"));
        assertThatThrownBy(() -> 
            retryService.executeWithRetry("test-topic", topicConfig, failingOperation, "test-operation"));

        // Then
        RetryService.RetryStats stats = retryService.getRetryStats("test-topic");
        assertThat(stats.getTotalFailures()).isEqualTo(2);
    }

    @Test
    void shouldProvideRetryStatsAccessors() {
        // Given
        RetryService.RetryStats stats = new RetryService.RetryStats(
            "test-topic", 2, 5, "Test error", LocalDateTime.now()
        );

        // Then
        assertThat(stats.getTopicName()).isEqualTo("test-topic");
        assertThat(stats.getCurrentAttempt()).isEqualTo(2);
        assertThat(stats.getTotalFailures()).isEqualTo(5);
        assertThat(stats.getLastError()).isEqualTo("Test error");
        assertThat(stats.getLastFailureTime()).isNotNull();
    }
}