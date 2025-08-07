package com.company.kafkaconnector.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import jakarta.annotation.PostConstruct;

/**
 * Logging configuration for structured logging and performance monitoring
 * Following configuration pattern with AOP enablement
 */
@Configuration
@EnableAspectJAutoProxy
@Slf4j
public class LoggingConfiguration {
    
    @PostConstruct
    public void initializeLogging() {
        log.info("Initializing enhanced logging configuration");
        log.info("Structured logging enabled with JSON format");
        log.info("Performance monitoring enabled via AOP");
        log.info("Correlation ID tracking enabled");
        
        // Log current logging configuration
        logLoggingConfiguration();
    }
    
    private void logLoggingConfiguration() {
        log.info("=== Logging Configuration ===");
        log.info("Profile-based log levels: DEBUG (non-prod), INFO (prod)");
        log.info("MDC Context Keys: correlationId, topic, partition, offset, operation, component");
        log.info("Performance Thresholds: S3_WRITE(5s), RECORD_PROCESSING(100ms), SCHEMA_VALIDATION(50ms)");
        log.info("Log Files: kafka-s3-connector.log, metrics.log, performance.log");
        log.info("Log Rotation: Daily with 30-day retention, 100MB max file size");
        log.info("Async Appenders: Enabled in production profile");
        log.info("=============================");
    }
}