package com.company.kafkaconnector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@Service
public class CircuitBreakerService {
    
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerService.class);
    
    private final ConcurrentHashMap<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    
    // Configuration constants
    private static final int FAILURE_THRESHOLD = 5; // Open circuit after 5 failures
    private static final Duration TIMEOUT_DURATION = Duration.ofMinutes(1); // Try again after 1 minute
    private static final int SUCCESS_THRESHOLD = 3; // Close circuit after 3 successes in half-open state
    
    public <T> T executeWithCircuitBreaker(String serviceName, Supplier<T> operation) {
        CircuitBreaker circuitBreaker = circuitBreakers.computeIfAbsent(serviceName, 
            k -> new CircuitBreaker(serviceName));
        
        return circuitBreaker.execute(operation);
    }
    
    public CircuitBreakerState getCircuitBreakerState(String serviceName) {
        CircuitBreaker circuitBreaker = circuitBreakers.get(serviceName);
        if (circuitBreaker == null) {
            return new CircuitBreakerState(serviceName, State.CLOSED, 0, 0, null);
        }
        
        return new CircuitBreakerState(
            serviceName,
            circuitBreaker.state,
            circuitBreaker.failureCount,
            circuitBreaker.successCount,
            circuitBreaker.lastFailureTime
        );
    }
    
    private class CircuitBreaker {
        private final String serviceName;
        private volatile State state = State.CLOSED;
        private volatile int failureCount = 0;
        private volatile int successCount = 0;
        private volatile LocalDateTime lastFailureTime;
        
        public CircuitBreaker(String serviceName) {
            this.serviceName = serviceName;
        }
        
        public <T> T execute(Supplier<T> operation) {
            if (state == State.OPEN) {
                if (shouldAttemptReset()) {
                    logger.info("Circuit breaker for {} transitioning to HALF_OPEN", serviceName);
                    state = State.HALF_OPEN;
                    successCount = 0;
                } else {
                    throw new CircuitBreakerOpenException("Circuit breaker is OPEN for service: " + serviceName);
                }
            }
            
            try {
                T result = operation.get();
                onSuccess();
                return result;
            } catch (Exception e) {
                onFailure();
                throw e;
            }
        }
        
        private boolean shouldAttemptReset() {
            return lastFailureTime != null && 
                   Duration.between(lastFailureTime, LocalDateTime.now()).compareTo(TIMEOUT_DURATION) > 0;
        }
        
        private void onSuccess() {
            if (state == State.HALF_OPEN) {
                successCount++;
                if (successCount >= SUCCESS_THRESHOLD) {
                    logger.info("Circuit breaker for {} transitioning to CLOSED", serviceName);
                    state = State.CLOSED;
                    failureCount = 0;
                    successCount = 0;
                }
            } else if (state == State.CLOSED) {
                // Reset failure count on success
                failureCount = 0;
            }
        }
        
        private void onFailure() {
            failureCount++;
            lastFailureTime = LocalDateTime.now();
            
            if (state == State.CLOSED && failureCount >= FAILURE_THRESHOLD) {
                logger.warn("Circuit breaker for {} transitioning to OPEN after {} failures", 
                    serviceName, failureCount);
                state = State.OPEN;
            } else if (state == State.HALF_OPEN) {
                logger.warn("Circuit breaker for {} transitioning back to OPEN", serviceName);
                state = State.OPEN;
                successCount = 0;
            }
        }
    }
    
    public enum State {
        CLOSED,    // Normal operation
        OPEN,      // Failing fast, not allowing calls
        HALF_OPEN  // Testing if the service has recovered
    }
    
    public static class CircuitBreakerState {
        private final String serviceName;
        private final State state;
        private final int failureCount;
        private final int successCount;
        private final LocalDateTime lastFailureTime;
        
        public CircuitBreakerState(String serviceName, State state, int failureCount, 
                                 int successCount, LocalDateTime lastFailureTime) {
            this.serviceName = serviceName;
            this.state = state;
            this.failureCount = failureCount;
            this.successCount = successCount;
            this.lastFailureTime = lastFailureTime;
        }
        
        // Getters
        public String getServiceName() { return serviceName; }
        public State getState() { return state; }
        public int getFailureCount() { return failureCount; }
        public int getSuccessCount() { return successCount; }
        public LocalDateTime getLastFailureTime() { return lastFailureTime; }
    }
    
    public static class CircuitBreakerOpenException extends RuntimeException {
        public CircuitBreakerOpenException(String message) {
            super(message);
        }
    }
}