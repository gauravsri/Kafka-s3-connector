# Reliability Guarantees

## Overview

The Kafka-S3-Delta Lake Connector provides comprehensive reliability guarantees to ensure data consistency and prevent message loss. This document outlines the mechanisms in place and their behavior.

## 📋 Table of Contents

1. [Message Processing Guarantees](#message-processing-guarantees)
2. [Error Classification](#error-classification)
3. [Retry Mechanisms](#retry-mechanisms)
4. [Idempotency and Duplicate Prevention](#idempotency-and-duplicate-prevention)
5. [Circuit Breaker Protection](#circuit-breaker-protection)
6. [Dead Letter Queue](#dead-letter-queue)
7. [Configuration](#configuration)
8. [Monitoring](#monitoring)

## Message Processing Guarantees

### At-Least-Once Delivery

The connector implements **at-least-once delivery** semantics with strong duplicate prevention:

- **Kafka Manual Acknowledgment**: Messages are only acknowledged after successful processing
- **Retriable Failures**: Messages are retried automatically for transient errors
- **Non-Retriable Failures**: Messages are sent to Dead Letter Queue to prevent infinite loops
- **Idempotency**: Duplicate detection prevents the same message from being processed multiple times

### Exactly-Once Processing

Through the combination of manual acknowledgment and idempotency mechanisms:

```java
// Messages are only acknowledged on success
if (result.isSuccess()) {
    acknowledgment.acknowledge();
} else if (isRetriable(error)) {
    // DO NOT acknowledge - Kafka will retry
    throw new RetriableException();
} else {
    // Send to DLQ and acknowledge to prevent blocking
    acknowledgment.acknowledge();
}
```

## Error Classification

The connector automatically classifies exceptions into **retriable** and **non-retriable** categories using the Kafka Connect pattern:

### Retriable Exceptions

**Network and I/O Issues:**
- `ConnectException`, `SocketTimeoutException`, `UnknownHostException`
- `TimeoutException`, `IOException`

**AWS/S3 Transient Issues:**
- `ServiceException`, `ThrottlingException`, `InternalServerError`
- `ServiceUnavailableException`, `SlowDownException`

**Delta Lake Transient Issues:**
- `ConcurrentModificationException`
- `FileNotFoundException` (for temporary file system issues)

**Message-Based Classification:**
- Messages containing: "timeout", "connection", "unavailable", "throttl", "rate limit"
- HTTP status codes: 429, 500, 502, 503, 504

### Non-Retriable Exceptions

**Schema and Data Issues:**
- Schema validation failures
- Malformed JSON messages
- Configuration errors

**Permanent AWS/S3 Issues:**
- Authentication failures
- Authorization errors
- Bucket not found

## Retry Mechanisms

### Exponential Backoff

The connector implements exponential backoff with jitter:

```java
long backoff = baseBackoffMs * (long) Math.pow(2, attempt - 1);
long jitter = (long) (backoff * 0.25 * (ThreadLocalRandom.current().nextDouble() - 0.5));
long finalBackoff = Math.min(backoff + jitter, 30000); // Cap at 30 seconds
```

**Default Configuration:**
- Base backoff: 1 second
- Max backoff: 30 seconds
- Max retries: 3 (configurable per topic)
- Jitter: ±25%

### Kafka-Level Retries

For retriable exceptions, the connector:
1. **Does NOT acknowledge** the Kafka message
2. **Throws the exception** to trigger Kafka consumer retry
3. **Applies backoff** between retry attempts
4. **Eventually sends to DLQ** after max retries exceeded

## Idempotency and Duplicate Prevention

### Duplicate Detection Strategy

The connector uses a multi-layered approach to prevent duplicates:

```java
String fingerprint = generateFingerprint(topic, partition, offset, messageContent);
```

**Fingerprint Components:**
1. **Kafka Coordinates**: `topic:partition:offset` (for Kafka-level uniqueness)
2. **Content Hash**: SHA-256 of message content (for content-based deduplication)
3. **Combined Hash**: Final fingerprint prevents duplicates across rebalances

### Cache Management

**Retention Policy:**
- **Duration**: 24 hours (configurable)
- **Cleanup**: Automatic every 60 minutes
- **Memory**: Concurrent hash map with periodic cleanup

**Duplicate Handling:**
```java
if (idempotencyService.isDuplicate(topic, partition, offset, messageContent)) {
    logger.info("Skipping duplicate message");
    return createSuccessResult(); // Return success for duplicates
}
```

### Exactly-Once Guarantees

The idempotency service provides:
- **Cross-restart persistence**: Survives application restarts (24h window)
- **Rebalance protection**: Handles partition reassignments
- **Content-based deduplication**: Detects identical messages across different offsets

## Circuit Breaker Protection

### Purpose

Protects the connector from cascading failures and provides graceful degradation:

- **Failure Threshold**: 5 consecutive failures
- **Timeout**: 1 minute before retry
- **Recovery Threshold**: 3 consecutive successes

### States

```
CLOSED (Normal) --[5 failures]--> OPEN (Failing Fast)
   ^                                      |
   |                                 [1 min timeout]
   |                                      v
   +--[3 successes]-- HALF_OPEN (Testing Recovery)
```

### Behavior

**OPEN State:**
- All requests fail immediately
- Messages sent to Dead Letter Queue
- No load on downstream services

**HALF_OPEN State:**
- Limited requests allowed
- Success closes circuit
- Failure reopens circuit

## Dead Letter Queue

### Purpose

Captures messages that cannot be processed after all retry attempts:

- **Non-retriable failures**: Immediate DLQ routing
- **Retry exhaustion**: After max retries exceeded
- **Circuit breaker open**: When service is unavailable

### DLQ Message Format

```json
{
  "original_topic": "user.events.v1",
  "original_partition": "0",
  "original_offset": 12345,
  "original_key": "user-123",
  "original_value": "{...}",
  "original_timestamp": "2024-01-15T10:30:00",
  "error_reason": "Schema validation failed",
  "error_message": "Required field 'user_id' is missing",
  "error_class": "NonRetriableException",
  "error_stack_trace": "...",
  "dlq_timestamp": "2024-01-15T10:30:01",
  "dlq_version": "1.0"
}
```

### DLQ Topic Naming

- **Pattern**: `{original-topic}-dlq`
- **Examples**: 
  - `user.events.v1` → `user.events.v1-dlq`
  - `orders.lifecycle.v2` → `orders.lifecycle.v2-dlq`

## Configuration

### Application Configuration

```yaml
connector:
  topics:
    user-events:
      processing:
        batch-size: 1000
        flush-interval: 60
        max-retries: 3  # Retry attempts per message
        
spring:
  kafka:
    consumer:
      enable-auto-commit: false  # Essential for manual acknowledgment
      max-poll-interval-ms: 300000  # 5 minutes
      session-timeout-ms: 30000     # 30 seconds
```

### Circuit Breaker Settings

Default configuration (currently hardcoded):
```java
private static final int FAILURE_THRESHOLD = 5;
private static final Duration TIMEOUT_DURATION = Duration.ofMinutes(1);
private static final int SUCCESS_THRESHOLD = 3;
```

### Idempotency Settings

Default configuration:
```java
private static final long CACHE_RETENTION_HOURS = 24;
private static final int CLEANUP_INTERVAL_MINUTES = 60;
```

## Monitoring

### Key Metrics

**Message Processing:**
- Messages processed successfully
- Messages failed (retriable vs non-retriable)
- Messages sent to DLQ
- Processing latency

**Reliability:**
- Retry attempts per topic
- Circuit breaker state changes
- Duplicate messages detected
- Idempotency cache hit rate

**Infrastructure:**
- Kafka consumer lag
- S3 write latency
- Delta Lake transaction success rate

### Health Checks

**Available Endpoints:**
- `/actuator/health` - Overall system health
- `/actuator/metrics` - Detailed metrics
- `/actuator/prometheus` - Prometheus metrics

**Health Indicators:**
- Kafka consumer connectivity
- S3/MinIO connectivity
- Circuit breaker states
- DLQ message rates

### Logging

**Log Levels:**
- `INFO`: Successful processing, retries, DLQ routing
- `WARN`: Retriable failures, circuit breaker state changes
- `ERROR`: Non-retriable failures, configuration issues
- `DEBUG`: Message details, idempotency checks

**Correlation IDs:**
All log messages include correlation IDs for request tracing:
```
2024-01-15 10:30:00.123 [kafka-consumer-1] INFO [c123-456] c.c.k.service.TopicRouterService - Processing message from topic: user.events.v1
```

## Failure Scenarios

### Scenario 1: Transient S3 Outage

**Behavior:**
1. S3 write fails with `ServiceException`
2. Classified as **retriable**
3. Message **not acknowledged**
4. Automatic retry with exponential backoff
5. Success after S3 recovery
6. Message acknowledged

**Result:** No data loss, automatic recovery

### Scenario 2: Invalid Message Schema

**Behavior:**
1. Schema validation fails
2. Classified as **non-retriable**
3. Message **immediately acknowledged**
4. Sent to DLQ for manual review
5. Processing continues with next message

**Result:** Bad message quarantined, pipeline continues

### Scenario 3: Consumer Rebalance

**Behavior:**
1. Kafka triggers consumer rebalance
2. In-flight messages not yet acknowledged
3. Messages reassigned to other consumers
4. Idempotency service prevents duplicate processing
5. Processing resumes normally

**Result:** Exactly-once processing maintained

### Scenario 4: Application Restart

**Behavior:**
1. Application shuts down gracefully
2. In-flight messages not acknowledged
3. Application restarts
4. Kafka redelivers unacknowledged messages
5. Idempotency cache prevents duplicates (24h window)

**Result:** No data loss, no duplicates

## Best Practices

### For Developers

1. **Exception Handling**: Always use `RetriableException` and `NonRetriableException`
2. **Idempotency**: Design operations to be naturally idempotent
3. **Monitoring**: Monitor DLQ topic sizes and error rates
4. **Testing**: Test failure scenarios and recovery behavior

### For Operations

1. **DLQ Monitoring**: Set up alerts for DLQ message accumulation
2. **Circuit Breaker**: Monitor circuit breaker state changes
3. **Consumer Lag**: Monitor Kafka consumer lag for early warning
4. **Resource Limits**: Ensure sufficient memory for idempotency cache

### For Configuration

1. **Batch Sizes**: Balance throughput vs memory usage
2. **Retry Limits**: Set appropriate max retries per error type
3. **Timeouts**: Configure consumer timeouts for your SLA requirements
4. **DLQ Retention**: Set appropriate retention for DLQ topics

## Recovery Procedures

### DLQ Message Reprocessing

Currently planned but not implemented. Will involve:
1. Manual review of DLQ messages
2. Fix underlying issues (schema, configuration)
3. Replay messages through normal pipeline
4. Verify successful processing

### Circuit Breaker Reset

If circuit breaker is stuck open:
1. Check downstream service health
2. Verify network connectivity
3. Review error logs for root cause
4. Circuit breaker auto-recovers after timeout

### Idempotency Cache Reset

If needed (rare):
1. Restart application to clear in-memory cache
2. Monitor for potential duplicates in 24h window
3. Consider Delta Lake ACID properties as backup

---

This reliability framework ensures the Kafka-S3-Delta Lake Connector provides enterprise-grade data consistency and fault tolerance.