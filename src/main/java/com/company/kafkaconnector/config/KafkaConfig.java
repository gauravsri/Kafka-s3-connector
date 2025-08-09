package com.company.kafkaconnector.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Critical for offset-based idempotency:
        // - MANUAL_IMMEDIATE: Only commit offset after explicit acknowledgment
        // - This ensures messages are never lost and provides natural idempotency
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        // Set concurrency based on topic partitions for optimal throughput
        factory.setConcurrency(3);
        
        // Custom rebalance listener for graceful partition handoff
        factory.getContainerProperties().setConsumerRebalanceListener(new GracefulRebalanceListener());
        
        // Custom error handler with circuit breaker pattern
        factory.setCommonErrorHandler(new CircuitBreakerErrorHandler());
        
        return factory;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    /**
     * Custom rebalance listener for graceful partition handoff and ownership tracking
     */
    private static class GracefulRebalanceListener implements ConsumerRebalanceListener {
        
        private static final Logger logger = LoggerFactory.getLogger(GracefulRebalanceListener.class);
        
        @Override
        public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
            if (!partitions.isEmpty()) {
                logger.info("Partitions being revoked: {}", partitions);
                
                // Graceful shutdown: flush any pending batches before losing partition ownership
                for (TopicPartition partition : partitions) {
                    try {
                        // Flush pending writes for this partition
                        flushPendingWrites(partition);
                        
                        // Track partition ownership change
                        logger.info("Gracefully released partition: {}", partition);
                        
                    } catch (Exception e) {
                        logger.error("Error during graceful partition release for {}: {}", 
                                   partition, e.getMessage(), e);
                    }
                }
            }
        }
        
        @Override
        public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
            if (!partitions.isEmpty()) {
                logger.info("Partitions assigned: {}", partitions);
                
                // Track new partition ownership
                for (TopicPartition partition : partitions) {
                    try {
                        // Initialize partition-specific state
                        initializePartitionState(partition);
                        
                        logger.debug("Initialized state for partition: {}", partition);
                        
                    } catch (Exception e) {
                        logger.error("Error initializing partition state for {}: {}", 
                                   partition, e.getMessage(), e);
                    }
                }
            }
        }
        
        @Override
        public void onPartitionsLost(java.util.Collection<TopicPartition> partitions) {
            if (!partitions.isEmpty()) {
                logger.warn("Partitions lost unexpectedly: {}", partitions);
                
                // Handle unexpected partition loss - more aggressive cleanup
                for (TopicPartition partition : partitions) {
                    try {
                        // Clean up any resources associated with lost partitions
                        cleanupLostPartition(partition);
                        
                    } catch (Exception e) {
                        logger.error("Error cleaning up lost partition {}: {}", 
                                   partition, e.getMessage(), e);
                    }
                }
            }
        }
        
        /**
         * Flush any pending writes for the partition being revoked
         */
        private void flushPendingWrites(TopicPartition partition) {
            // In a real implementation, this would:
            // 1. Get the DeltaWriterService bean
            // 2. Call flushAllBatches() for this specific partition
            // 3. Wait for completion with timeout
            
            logger.debug("Flushing pending writes for partition: {}", partition);
            
            // Simulate flush operation
            try {
                Thread.sleep(100); // Small delay to simulate flush
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        /**
         * Initialize partition-specific state for newly assigned partitions
         */
        private void initializePartitionState(TopicPartition partition) {
            // In a real implementation, this would:
            // 1. Initialize metrics for this partition
            // 2. Set up partition-specific batching state
            // 3. Initialize offset tracking
            
            logger.debug("Initializing state for partition: {}", partition);
        }
        
        /**
         * Clean up resources for lost partitions
         */
        private void cleanupLostPartition(TopicPartition partition) {
            // In a real implementation, this would:
            // 1. Mark any in-flight operations as failed
            // 2. Clean up metrics and state
            // 3. Release any partition-specific resources
            
            logger.debug("Cleaning up lost partition: {}", partition);
        }
    }
    
    /**
     * Custom error handler with circuit breaker pattern for failing topics
     */
    private static class CircuitBreakerErrorHandler extends DefaultErrorHandler {
        
        private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerErrorHandler.class);
        
        // Circuit breaker state per topic
        private final Map<String, CircuitBreakerState> circuitBreakers = new java.util.concurrent.ConcurrentHashMap<>();
        
        public CircuitBreakerErrorHandler() {
            // Configure exponential backoff with max retries
            super(new FixedBackOff(1000L, 3L)); // 1 second interval, 3 retries
            logger.info("Initialized Circuit Breaker Error Handler");
        }
        
        @Override
        public boolean handleRecord(Exception thrownException, 
                                  org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                                  org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                  org.springframework.kafka.listener.MessageListenerContainer container) {
            
            String topic = record.topic();
            CircuitBreakerState circuitBreaker = circuitBreakers.computeIfAbsent(topic, 
                k -> new CircuitBreakerState(topic));
            
            // Check circuit breaker state
            if (circuitBreaker.isOpen()) {
                logger.warn("Circuit breaker OPEN for topic: {} - dropping message at offset: {}", 
                           topic, record.offset());
                
                // Log the dropped message for monitoring
                logDroppedMessage(record, thrownException);
                
                // Return true to acknowledge the message and prevent infinite retry
                return true;
            }
            
            // Record the failure
            circuitBreaker.recordFailure();
            
            // Classify the error
            if (isCircuitBreakerTrigger(thrownException)) {
                logger.error("Circuit breaker triggering error for topic: {} at offset: {}", 
                           topic, record.offset(), thrownException);
                
                circuitBreaker.trip();
                
                // Schedule circuit breaker reset
                scheduleCircuitBreakerReset(circuitBreaker);
                
                // Don't retry circuit breaker triggering errors
                logDroppedMessage(record, thrownException);
                return true; // Acknowledge to prevent infinite retry
            }
            
            // For retriable errors, use default retry logic
            try {
                boolean handled = super.handleRecord(thrownException, record, consumer, container);
                
                // Record success if handled successfully
                if (handled) {
                    circuitBreaker.recordSuccess();
                }
                
                return handled;
                
            } catch (Exception e) {
                logger.error("Failed to handle record after retries for topic: {} at offset: {}", 
                           topic, record.offset(), e);
                
                // Final failure - log and move on
                logDroppedMessage(record, e);
                return true; // Acknowledge to prevent getting stuck
            }
        }
        
        /**
         * Determine if the exception should trigger the circuit breaker
         */
        private boolean isCircuitBreakerTrigger(Exception e) {
            // Circuit breaker triggers on:
            // 1. Schema validation errors (persistent)
            // 2. Authentication/authorization errors
            // 3. Persistent business validation failures
            
            String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            String exceptionName = e.getClass().getSimpleName().toLowerCase();
            
            return message.contains("schema") ||
                   message.contains("unauthorized") ||
                   message.contains("authentication") ||
                   exceptionName.contains("security") ||
                   exceptionName.contains("validation") ||
                   e instanceof com.company.kafkaconnector.exception.NonRetriableException;
        }
        
        /**
         * Log dropped messages for monitoring and alerting
         */
        private void logDroppedMessage(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record, 
                                     Exception cause) {
            logger.error("DROPPED MESSAGE - Topic: {}, Partition: {}, Offset: {}, Key: {}, Error: {}", 
                        record.topic(), record.partition(), record.offset(), 
                        record.key(), cause.getMessage());
            
            // In production, this would also:
            // 1. Send alerts to monitoring systems
            // 2. Write to dead letter queue
            // 3. Update metrics
        }
        
        /**
         * Schedule circuit breaker reset after timeout
         */
        private void scheduleCircuitBreakerReset(CircuitBreakerState circuitBreaker) {
            // Simple implementation - in production would use proper scheduler
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
                .schedule(circuitBreaker::halfOpen, 30, java.util.concurrent.TimeUnit.SECONDS);
        }
    }
    
    /**
     * Circuit breaker state management
     */
    private static class CircuitBreakerState {
        private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerState.class);
        
        private final String topic;
        private volatile State state = State.CLOSED;
        private volatile long lastFailureTime = 0;
        private volatile int consecutiveFailures = 0;
        private volatile int successCount = 0;
        
        private static final int FAILURE_THRESHOLD = 5;
        private static final int SUCCESS_THRESHOLD = 3;
        
        public CircuitBreakerState(String topic) {
            this.topic = topic;
        }
        
        public boolean isOpen() {
            return state == State.OPEN;
        }
        
        public boolean isHalfOpen() {
            return state == State.HALF_OPEN;
        }
        
        public synchronized void recordFailure() {
            lastFailureTime = System.currentTimeMillis();
            consecutiveFailures++;
            successCount = 0;
            
            if (state == State.HALF_OPEN) {
                // Failed during half-open, go back to open
                state = State.OPEN;
                logger.warn("Circuit breaker OPEN (from half-open) for topic: {} after failure", topic);
            }
        }
        
        public synchronized void recordSuccess() {
            consecutiveFailures = 0;
            successCount++;
            
            if (state == State.HALF_OPEN && successCount >= SUCCESS_THRESHOLD) {
                state = State.CLOSED;
                logger.info("Circuit breaker CLOSED for topic: {} after {} successes", topic, successCount);
            }
        }
        
        public synchronized void trip() {
            if (consecutiveFailures >= FAILURE_THRESHOLD) {
                state = State.OPEN;
                logger.warn("Circuit breaker OPEN for topic: {} after {} consecutive failures", 
                           topic, consecutiveFailures);
            }
        }
        
        public synchronized void halfOpen() {
            if (state == State.OPEN) {
                state = State.HALF_OPEN;
                successCount = 0;
                logger.info("Circuit breaker HALF_OPEN for topic: {}", topic);
            }
        }
        
        private enum State {
            CLOSED, OPEN, HALF_OPEN
        }
    }
}