package com.company.kafkaconnector.service;

import com.company.kafkaconnector.model.DeltaConfig;
import com.company.kafkaconnector.model.TopicConfig;
import com.company.kafkaconnector.model.DestinationConfig;
import io.delta.kernel.engine.Engine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class DeltaOptimizationServiceTest {

    @Mock
    private Engine mockEngine;

    private DeltaOptimizationService optimizationService;
    private TopicConfig testTopicConfig;
    private String testTablePath;

    @BeforeEach
    void setUp() {
        optimizationService = new DeltaOptimizationService(mockEngine);
        
        // Create test topic config
        DeltaConfig deltaConfig = new DeltaConfig(true, 5, false, 168, true, "10");
        DestinationConfig destination = new DestinationConfig();
        destination.setBucket("test-bucket");
        destination.setPath("test/path");
        destination.setTableName("test_table");
        destination.setPartitionColumns(List.of("year", "month", "day"));
        destination.setDeltaConfig(deltaConfig);
        
        testTopicConfig = new TopicConfig();
        testTopicConfig.setKafkaTopic("test.topic.v1");
        testTopicConfig.setDestination(destination);
        
        testTablePath = "s3a://test-bucket/test/path";
    }

    @Test
    void shouldTrackBatchWrites() {
        // When
        optimizationService.trackBatchWrite(testTablePath);
        optimizationService.trackBatchWrite(testTablePath);
        optimizationService.trackBatchWrite(testTablePath);

        // Then
        Map<String, Object> stats = optimizationService.getOptimizationStats(testTablePath);
        assertThat(stats.get("batchesSinceOptimize")).isEqualTo(3);
    }

    @Test
    void shouldOptimizeWhenIntervalReached() {
        // Given - optimize interval is 5
        optimizationService.trackBatchWrite(testTablePath);
        optimizationService.trackBatchWrite(testTablePath);
        optimizationService.trackBatchWrite(testTablePath);
        optimizationService.trackBatchWrite(testTablePath);
        
        // When - before threshold
        boolean shouldOptimize = optimizationService.shouldOptimize(testTopicConfig, testTablePath);
        assertThat(shouldOptimize).isFalse();

        // When - at threshold
        optimizationService.trackBatchWrite(testTablePath);
        shouldOptimize = optimizationService.shouldOptimize(testTopicConfig, testTablePath);
        
        // Then
        assertThat(shouldOptimize).isTrue();
    }

    @Test
    void shouldNotOptimizeWhenDisabled() {
        // Given
        testTopicConfig.getDestination().getDeltaConfig().setEnableOptimize(false);
        
        // Track enough batches to trigger optimization
        for (int i = 0; i < 10; i++) {
            optimizationService.trackBatchWrite(testTablePath);
        }

        // When
        boolean shouldOptimize = optimizationService.shouldOptimize(testTopicConfig, testTablePath);

        // Then
        assertThat(shouldOptimize).isFalse();
    }

    @Test
    void shouldVacuumWhenEnabled() {
        // Given
        testTopicConfig.getDestination().getDeltaConfig().setEnableVacuum(true);

        // When - first time
        boolean shouldVacuum = optimizationService.shouldVacuum(testTopicConfig, testTablePath);

        // Then - should vacuum on first check
        assertThat(shouldVacuum).isTrue();
    }

    @Test
    void shouldNotVacuumWhenDisabled() {
        // Given
        testTopicConfig.getDestination().getDeltaConfig().setEnableVacuum(false);

        // When
        boolean shouldVacuum = optimizationService.shouldVacuum(testTopicConfig, testTablePath);

        // Then
        assertThat(shouldVacuum).isFalse();
    }

    @Test
    void shouldResetCountersAfterOptimization() {
        // Given
        optimizationService.trackBatchWrite(testTablePath);
        optimizationService.trackBatchWrite(testTablePath);
        optimizationService.trackBatchWrite(testTablePath);

        // When
        CompletableFuture<Void> optimizeFuture = optimizationService.optimizeTable(testTopicConfig, testTablePath);
        optimizeFuture.join(); // Wait for completion

        // Then
        // Note: With a mock engine, the operation will fail but counters should still be reset
        // In a real implementation with proper engine, this would succeed
        Map<String, Object> stats = optimizationService.getOptimizationStats(testTablePath);
        // Counter reset happens even if optimization fails (due to error handling)
        assertThat(stats.get("batchesSinceOptimize")).isEqualTo(0);
        assertThat(stats.containsKey("lastOptimizeTime")).isTrue();
    }

    @Test
    void shouldUpdateVacuumTimestamp() {
        // Given
        testTopicConfig.getDestination().getDeltaConfig().setEnableVacuum(true);

        // When
        CompletableFuture<Void> vacuumFuture = optimizationService.vacuumTable(testTopicConfig, testTablePath);
        vacuumFuture.join(); // Wait for completion

        // Then
        Map<String, Object> stats = optimizationService.getOptimizationStats(testTablePath);
        assertThat(stats.containsKey("lastVacuumTime")).isTrue();
    }

    @Test
    void shouldPerformOptimizationsWhenNeeded() {
        // Given - setup for optimization
        for (int i = 0; i < 5; i++) {
            optimizationService.trackBatchWrite(testTablePath);
        }

        // When
        optimizationService.performOptimizations(testTopicConfig);

        // Give async operations time to complete
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Then - counters should be reset
        Map<String, Object> stats = optimizationService.getOptimizationStats(testTablePath);
        assertThat(stats.get("batchesSinceOptimize")).isEqualTo(0);
        assertThat(stats.containsKey("lastOptimizeTime")).isTrue();
    }

    @Test
    void shouldResetAllCounters() {
        // Given
        optimizationService.trackBatchWrite(testTablePath);
        optimizationService.trackBatchWrite("another-path");

        // When
        optimizationService.resetOptimizationCounters();

        // Then
        Map<String, Object> stats = optimizationService.getOptimizationStats(testTablePath);
        assertThat(stats.get("batchesSinceOptimize")).isEqualTo(0);
        assertThat(stats.containsKey("lastOptimizeTime")).isFalse();
        assertThat(stats.containsKey("lastVacuumTime")).isFalse();
    }

    @Test
    void shouldHandleMultipleTablesIndependently() {
        // Given
        String tablePath1 = "s3a://bucket1/path1";
        String tablePath2 = "s3a://bucket2/path2";

        // When
        optimizationService.trackBatchWrite(tablePath1);
        optimizationService.trackBatchWrite(tablePath1);
        optimizationService.trackBatchWrite(tablePath2);

        // Then
        Map<String, Object> stats1 = optimizationService.getOptimizationStats(tablePath1);
        Map<String, Object> stats2 = optimizationService.getOptimizationStats(tablePath2);
        
        assertThat(stats1.get("batchesSinceOptimize")).isEqualTo(2);
        assertThat(stats2.get("batchesSinceOptimize")).isEqualTo(1);
    }

    @Test
    void shouldReturnEmptyStatsForUnknownTable() {
        // When
        Map<String, Object> stats = optimizationService.getOptimizationStats("unknown-path");

        // Then
        assertThat(stats.get("batchesSinceOptimize")).isEqualTo(0);
        assertThat(stats.containsKey("lastOptimizeTime")).isFalse();
        assertThat(stats.containsKey("lastVacuumTime")).isFalse();
    }

    @Test
    void shouldHandleConcurrentBatchTracking() throws InterruptedException {
        // Given
        int numThreads = 5;
        int batchesPerThread = 10;
        Thread[] threads = new Thread[numThreads];

        // When
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < batchesPerThread; j++) {
                    optimizationService.trackBatchWrite(testTablePath);
                }
            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Then
        Map<String, Object> stats = optimizationService.getOptimizationStats(testTablePath);
        assertThat(stats.get("batchesSinceOptimize")).isEqualTo(numThreads * batchesPerThread);
    }

    @Test
    void shouldHandleOptimizationErrors() {
        // Given - This test verifies that errors during optimization don't crash the service
        String invalidTablePath = "invalid://path";

        // When/Then - should not throw exception
        assertDoesNotThrow(() -> {
            CompletableFuture<Void> future = optimizationService.optimizeTable(testTopicConfig, invalidTablePath);
            future.join();
        });
    }

    @Test
    void shouldHandleVacuumErrors() {
        // Given - This test verifies that errors during vacuum don't crash the service
        String invalidTablePath = "invalid://path";
        testTopicConfig.getDestination().getDeltaConfig().setEnableVacuum(true);

        // When/Then - should not throw exception
        assertDoesNotThrow(() -> {
            CompletableFuture<Void> future = optimizationService.vacuumTable(testTopicConfig, invalidTablePath);
            future.join();
        });
    }
}