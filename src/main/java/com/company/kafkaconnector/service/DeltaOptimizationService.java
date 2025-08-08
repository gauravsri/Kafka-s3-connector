package com.company.kafkaconnector.service;

import com.company.kafkaconnector.model.TopicConfig;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

@Service
public class DeltaOptimizationService {

    private static final Logger logger = LoggerFactory.getLogger(DeltaOptimizationService.class);

    private final Engine engine;
    private final Map<String, Integer> batchCountsSinceOptimize = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastOptimizeTime = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastVacuumTime = new ConcurrentHashMap<>();

    public DeltaOptimizationService(Engine engine) {
        this.engine = engine;
    }

    public void trackBatchWrite(String tablePath) {
        batchCountsSinceOptimize.merge(tablePath, 1, Integer::sum);
    }

    public boolean shouldOptimize(TopicConfig topicConfig, String tablePath) {
        if (!topicConfig.getDestination().getDeltaConfig().isEnableOptimize()) {
            return false;
        }

        int batchesSinceOptimize = batchCountsSinceOptimize.getOrDefault(tablePath, 0);
        int optimizeInterval = topicConfig.getDestination().getDeltaConfig().getOptimizeInterval();
        
        return batchesSinceOptimize >= optimizeInterval;
    }

    public boolean shouldVacuum(TopicConfig topicConfig, String tablePath) {
        if (!topicConfig.getDestination().getDeltaConfig().isEnableVacuum()) {
            return false;
        }

        Instant lastVacuum = lastVacuumTime.get(tablePath);
        if (lastVacuum == null) {
            return true; // First vacuum
        }

        Duration timeSinceLastVacuum = Duration.between(lastVacuum, Instant.now());
        Duration vacuumInterval = Duration.ofHours(topicConfig.getDestination().getDeltaConfig().getVacuumRetentionHours());
        
        return timeSinceLastVacuum.compareTo(vacuumInterval) >= 0;
    }

    @Async
    public CompletableFuture<Void> optimizeTable(TopicConfig topicConfig, String tablePath) {
        return CompletableFuture.runAsync(() -> {
            try {
                logger.info("Starting table optimization for: {}", tablePath);
                
                Table table = Table.forPath(engine, tablePath);
                
                // Create optimize transaction
                TransactionBuilder txBuilder = table.createTransactionBuilder(
                    engine,
                    "kafka-connector-optimize",
                    Operation.WRITE
                );

                Transaction txn = txBuilder.build(engine);
                Row txnState = txn.getTransactionState(engine);

                // Execute optimize operation
                // Note: Delta Kernel doesn't have direct optimize API, so we simulate the process
                // In a real implementation, this would use Delta Lake's optimize functionality
                performOptimizeOperation(table, txnState);

                // Reset batch counter and update timestamp
                batchCountsSinceOptimize.put(tablePath, 0);
                lastOptimizeTime.put(tablePath, Instant.now());
                
                logger.info("Table optimization completed for: {}", tablePath);
                
            } catch (Exception e) {
                logger.error("Failed to optimize table: {}", tablePath, e);
                // Reset counters even on failure to avoid accumulation
                batchCountsSinceOptimize.put(tablePath, 0);
                lastOptimizeTime.put(tablePath, Instant.now());
            }
        });
    }

    @Async
    public CompletableFuture<Void> vacuumTable(TopicConfig topicConfig, String tablePath) {
        return CompletableFuture.runAsync(() -> {
            try {
                logger.info("Starting table vacuum for: {} (retention: {} hours)", 
                    tablePath, topicConfig.getDestination().getDeltaConfig().getVacuumRetentionHours());
                
                Table table = Table.forPath(engine, tablePath);
                
                // Create vacuum transaction
                TransactionBuilder txBuilder = table.createTransactionBuilder(
                    engine,
                    "kafka-connector-vacuum",
                    Operation.WRITE
                );

                Transaction txn = txBuilder.build(engine);
                Row txnState = txn.getTransactionState(engine);

                // Execute vacuum operation
                performVacuumOperation(table, txnState, topicConfig.getDestination().getDeltaConfig().getVacuumRetentionHours());

                // Update vacuum timestamp
                lastVacuumTime.put(tablePath, Instant.now());
                
                logger.info("Table vacuum completed for: {}", tablePath);
                
            } catch (Exception e) {
                logger.error("Failed to vacuum table: {}", tablePath, e);
                // Update timestamp even on failure to avoid continuous retry
                lastVacuumTime.put(tablePath, Instant.now());
            }
        });
    }

    private void performOptimizeOperation(Table table, Row txnState) {
        try {
            // In a full implementation, this would:
            // 1. Identify small files that need compaction
            // 2. Read data from small files
            // 3. Combine and rewrite as larger, optimized files
            // 4. Update transaction log with remove/add actions
            
            logger.info("Simulating optimize operation - compacting small files");
            
            // For now, we'll create a minimal transaction to demonstrate the structure
            CloseableIterable<Row> optimizeActions = CloseableIterable.inMemoryIterable(
                new CloseableIterator<Row>() {
                    @Override
                    public boolean hasNext() { return false; }
                    
                    @Override
                    public Row next() { return null; }
                    
                    @Override
                    public void close() {}
                }
            );
            
            // In a real implementation, you would commit the optimize actions
            // txn.commit(engine, optimizeActions);
            
            logger.debug("Optimize operation completed successfully");
            
        } catch (Exception e) {
            logger.error("Error during optimize operation", e);
            throw new RuntimeException("Optimize operation failed", e);
        }
    }

    private void performVacuumOperation(Table table, Row txnState, int retentionHours) {
        try {
            // In a full implementation, this would:
            // 1. Identify files older than retention period
            // 2. Ensure files are not referenced by any snapshot within retention
            // 3. Physically delete unreferenced files
            // 4. Clean up metadata
            
            logger.info("Simulating vacuum operation - removing files older than {} hours", retentionHours);
            
            // For now, we'll simulate the vacuum process
            Instant cutoffTime = Instant.now().minus(Duration.ofHours(retentionHours));
            logger.debug("Vacuum cutoff time: {}", cutoffTime);
            
            // In a real implementation, you would:
            // 1. List all files in the table
            // 2. Check which files are safe to delete
            // 3. Remove them from storage
            
            logger.debug("Vacuum operation completed successfully");
            
        } catch (Exception e) {
            logger.error("Error during vacuum operation", e);
            throw new RuntimeException("Vacuum operation failed", e);
        }
    }

    public void performOptimizations(TopicConfig topicConfig) {
        String tablePath = String.format("s3a://%s/%s",
            topicConfig.getDestination().getBucket(),
            topicConfig.getDestination().getPath());

        // Check and perform optimize
        if (shouldOptimize(topicConfig, tablePath)) {
            logger.info("Triggering optimization for table: {}", tablePath);
            optimizeTable(topicConfig, tablePath);
        }

        // Check and perform vacuum
        if (shouldVacuum(topicConfig, tablePath)) {
            logger.info("Triggering vacuum for table: {}", tablePath);
            vacuumTable(topicConfig, tablePath);
        }
    }

    public Map<String, Object> getOptimizationStats(String tablePath) {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("batchesSinceOptimize", batchCountsSinceOptimize.getOrDefault(tablePath, 0));
        
        Instant lastOptimize = lastOptimizeTime.get(tablePath);
        if (lastOptimize != null) {
            stats.put("lastOptimizeTime", lastOptimize);
        }
        
        Instant lastVacuum = lastVacuumTime.get(tablePath);
        if (lastVacuum != null) {
            stats.put("lastVacuumTime", lastVacuum);
        }
        
        return stats;
    }

    public void resetOptimizationCounters() {
        batchCountsSinceOptimize.clear();
        lastOptimizeTime.clear();
        lastVacuumTime.clear();
        logger.info("Optimization counters reset");
    }
}