package com.company.kafkaconnector.logging;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * Aspect for performance logging of critical operations
 * Following AOP pattern for cross-cutting concerns
 */
@Aspect
@Component
@Slf4j
public class PerformanceLoggingAspect {
    
    private static final Logger performanceLogger = LoggerFactory.getLogger("PERFORMANCE");
    
    /**
     * Log performance for S3 write operations
     */
    @Around("execution(* com.company.kafkaconnector.writer.*.write(..))")
    public Object logS3WritePerformance(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMethodPerformance(joinPoint, "S3_WRITE");
    }
    
    /**
     * Log performance for record processing operations
     */
    @Around("execution(* com.company.kafkaconnector.service.RecordProcessor.process(..))")
    public Object logRecordProcessingPerformance(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMethodPerformance(joinPoint, "RECORD_PROCESSING");
    }
    
    /**
     * Log performance for schema validation
     */
    @Around("execution(* com.company.kafkaconnector.service.SchemaValidationService.validateMessage(..))")
    public Object logSchemaValidationPerformance(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMethodPerformance(joinPoint, "SCHEMA_VALIDATION");
    }
    
    /**
     * Log performance for connector task operations
     */
    @Around("execution(* com.company.kafkaconnector.connector.S3DeltaSinkTask.put(..))")
    public Object logTaskPutPerformance(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMethodPerformance(joinPoint, "TASK_PUT");
    }
    
    /**
     * Log performance for flush operations
     */
    @Around("execution(* com.company.kafkaconnector.connector.S3DeltaSinkTask.flush(..))")
    public Object logTaskFlushPerformance(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMethodPerformance(joinPoint, "TASK_FLUSH");
    }
    
    /**
     * Generic method performance logging
     */
    private Object logMethodPerformance(ProceedingJoinPoint joinPoint, String operationType) throws Throwable {
        String className = joinPoint.getTarget().getClass().getSimpleName();
        String methodName = joinPoint.getSignature().getName();
        Object[] args = joinPoint.getArgs();
        
        long startTime = System.currentTimeMillis();
        LoggingContext.setOperationContext(operationType, className);
        
        try {
            log.debug("Starting {} operation: {}.{} with {} arguments", 
                     operationType, className, methodName, args.length);
            
            Object result = joinPoint.proceed();
            
            long duration = System.currentTimeMillis() - startTime;
            LoggingContext.setPerformanceContext(duration);
            
            performanceLogger.info("Operation completed: {} - {}.{} took {}ms", 
                                 operationType, className, methodName, duration);
            
            if (duration > getWarningThreshold(operationType)) {
                log.warn("Slow operation detected: {} - {}.{} took {}ms (threshold: {}ms)", 
                        operationType, className, methodName, duration, getWarningThreshold(operationType));
            }
            
            return result;
            
        } catch (Throwable throwable) {
            long duration = System.currentTimeMillis() - startTime;
            LoggingContext.setPerformanceContext(duration);
            LoggingContext.setErrorContext(throwable.getClass().getSimpleName(), null);
            
            performanceLogger.error("Operation failed: {} - {}.{} failed after {}ms with error: {}", 
                                  operationType, className, methodName, duration, throwable.getMessage());
            
            throw throwable;
        } finally {
            LoggingContext.clearProcessingContext();
            LoggingContext.clearErrorContext();
        }
    }
    
    /**
     * Get warning threshold for different operation types
     */
    private long getWarningThreshold(String operationType) {
        return switch (operationType) {
            case "S3_WRITE" -> 5000L; // 5 seconds
            case "RECORD_PROCESSING" -> 100L; // 100ms
            case "SCHEMA_VALIDATION" -> 50L; // 50ms
            case "TASK_PUT" -> 1000L; // 1 second
            case "TASK_FLUSH" -> 10000L; // 10 seconds
            default -> 1000L;
        };
    }
    
    /**
     * Log batch operation metrics
     */
    @Around("execution(* com.company.kafkaconnector.connector.S3DeltaSinkTask.put(..)) && args(records)")
    public Object logBatchMetrics(ProceedingJoinPoint joinPoint, java.util.Collection<?> records) throws Throwable {
        int batchSize = records != null ? records.size() : 0;
        LoggingContext.setProcessingContext(batchSize, null, (long) batchSize);
        
        try {
            performanceLogger.info("Processing batch of {} records", batchSize);
            return joinPoint.proceed();
        } finally {
            LoggingContext.clearProcessingContext();
        }
    }
}