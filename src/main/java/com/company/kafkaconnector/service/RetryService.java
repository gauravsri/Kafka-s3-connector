package com.company.kafkaconnector.service;

import com.company.kafkaconnector.model.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

@Service
public class RetryService {
    
    private static final Logger logger = LoggerFactory.getLogger(RetryService.class);
    
    // Track retry attempts per topic
    private final ConcurrentHashMap<String, RetryState> retryStates = new ConcurrentHashMap<>();
    
    public <T> T executeWithRetry(String topicName, TopicConfig topicConfig, 
                                 Supplier<T> operation, String operationName) {
        
        int maxRetries = topicConfig.getProcessing().getMaxRetries();
        int attempt = 0;
        Exception lastException = null;
        
        while (attempt <= maxRetries) {
            try {
                T result = operation.get();
                
                // Success - reset retry state
                resetRetryState(topicName);
                
                if (attempt > 0) {
                    logger.info("Operation {} succeeded for topic {} after {} retries", 
                        operationName, topicName, attempt);
                }
                
                return result;
                
            } catch (Exception e) {
                lastException = e;
                attempt++;
                
                if (attempt <= maxRetries) {
                    long backoffMs = calculateBackoff(attempt);
                    
                    logger.warn("Operation {} failed for topic {} (attempt {}/{}). Retrying in {}ms. Error: {}", 
                        operationName, topicName, attempt, maxRetries + 1, backoffMs, e.getMessage());
                    
                    updateRetryState(topicName, attempt, e);
                    
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                } else {
                    logger.error("Operation {} failed for topic {} after {} attempts", 
                        operationName, topicName, maxRetries + 1, e);
                    
                    markFailure(topicName, e);
                }
            }
        }
        
        throw new RuntimeException("Operation failed after " + (maxRetries + 1) + " attempts", lastException);
    }
    
    private long calculateBackoff(int attempt) {
        // Exponential backoff with jitter: base * 2^(attempt-1) + jitter
        long baseBackoffMs = 1000; // 1 second base
        long backoff = baseBackoffMs * (long) Math.pow(2, attempt - 1);
        
        // Add jitter (±25%)
        long jitter = (long) (backoff * 0.25 * (ThreadLocalRandom.current().nextDouble() - 0.5));
        
        return Math.min(backoff + jitter, 30000); // Cap at 30 seconds
    }
    
    private void resetRetryState(String topicName) {
        retryStates.remove(topicName);
    }
    
    private void updateRetryState(String topicName, int attempt, Exception error) {
        RetryState state = retryStates.computeIfAbsent(topicName, k -> new RetryState());
        state.attempt = attempt;
        state.lastError = error.getMessage();
        state.lastAttemptTime = LocalDateTime.now();
    }
    
    private void markFailure(String topicName, Exception error) {
        RetryState state = retryStates.computeIfAbsent(topicName, k -> new RetryState());
        state.totalFailures++;
        state.lastFailureTime = LocalDateTime.now();
        state.lastError = error.getMessage();
    }
    
    public RetryStats getRetryStats(String topicName) {
        RetryState state = retryStates.get(topicName);
        if (state == null) {
            return new RetryStats(topicName, 0, 0, null, null);
        }
        
        return new RetryStats(
            topicName,
            state.attempt,
            state.totalFailures,
            state.lastError,
            state.lastFailureTime
        );
    }
    
    private static class RetryState {
        int attempt = 0;
        int totalFailures = 0;
        String lastError;
        LocalDateTime lastAttemptTime;
        LocalDateTime lastFailureTime;
    }
    
    public static class RetryStats {
        private final String topicName;
        private final int currentAttempt;
        private final int totalFailures;
        private final String lastError;
        private final LocalDateTime lastFailureTime;
        
        public RetryStats(String topicName, int currentAttempt, int totalFailures, 
                         String lastError, LocalDateTime lastFailureTime) {
            this.topicName = topicName;
            this.currentAttempt = currentAttempt;
            this.totalFailures = totalFailures;
            this.lastError = lastError;
            this.lastFailureTime = lastFailureTime;
        }
        
        // Getters
        public String getTopicName() { return topicName; }
        public int getCurrentAttempt() { return currentAttempt; }
        public int getTotalFailures() { return totalFailures; }
        public String getLastError() { return lastError; }
        public LocalDateTime getLastFailureTime() { return lastFailureTime; }
    }
}