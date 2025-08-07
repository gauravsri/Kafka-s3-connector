# Kafka to S3 Delta Lake Connector - Implementation Guide

## Architecture Overview

### Technology Stack
- **Framework**: Spring Boot 3.x
- **Kafka Integration**: Spring Kafka
- **Delta Lake**: Delta-core Java library
- **S3 Integration**: AWS SDK v2
- **Schema Management**: JSON Schema Validator
- **Build Tool**: Maven
- **Containerization**: Docker
- **Monitoring**: Micrometer + Prometheus

### Core Components Architecture
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Multiple Kafka  │───▶│ Topic Router &   │───▶│ Schema Validator│
│ Topics (A,B,C)  │    │ Message Handler  │    │ (per topic)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ S3 + Delta      │◀───│ Multi-Destination│◀───│ Message         │
│ Lake Storage    │    │ Delta Writer     │    │ Transformer     │
│ /topicA/data/   │    │ Service          │    │ (per topic)     │
│ /topicB/events/ │    └──────────────────┘    └─────────────────┘
│ /topicC/logs/   │
└─────────────────┘
```

## Implementation Steps

### Phase 1: Project Setup and Dependencies

#### Step 1.1: Initialize Spring Boot Project
```bash
# Use Spring Initializr or create manually
# Dependencies: Web, Actuator, Validation
```

#### Step 1.2: Add Maven Dependencies
Key dependencies to include in `pom.xml`:
- `spring-kafka` for Kafka integration
- `delta-core_2.12` for Delta Lake operations
- `software.amazon.awssdk:s3` for S3 operations
- `com.fasterxml.jackson.core:jackson-databind` for JSON processing
- `org.everit.json:org.everit.json.schema` for schema validation
- `micrometer-registry-prometheus` for metrics

#### Step 1.3: Project Structure
```
src/main/java/com/company/kafkaconnector/
├── KafkaConnectorApplication.java
├── config/
│   ├── KafkaConfig.java
│   ├── S3Config.java
│   ├── DeltaConfig.java
│   └── TopicConfiguration.java
├── service/
│   ├── TopicRouterService.java
│   ├── KafkaConsumerService.java
│   ├── SchemaValidationService.java
│   ├── MessageTransformationService.java
│   └── MultiDestinationDeltaWriterService.java
├── model/
│   ├── TopicConfig.java
│   ├── EventMessage.java
│   ├── ProcessingResult.java
│   └── DestinationConfig.java
├── exception/
│   └── ConnectorException.java
└── health/
    └── ConnectorHealthIndicator.java
```

### Phase 2: Configuration Management

#### Step 2.1: Multi-Topic Configuration Structure
Create `application.yml` with support for multiple topic configurations:

```yaml
kafka:
  bootstrap-servers: localhost:9092
  consumer:
    group-id: kafka-s3-connector
    auto-offset-reset: earliest
    enable-auto-commit: false

s3:
  region: us-west-2
  access-key-id: ${AWS_ACCESS_KEY_ID}
  secret-access-key: ${AWS_SECRET_ACCESS_KEY}

connector:
  topics:
    user-events:
      kafka-topic: "user.events.v1"
      schema-file: "schemas/user-events-schema.json"
      destination:
        bucket: "data-lake"
        path: "events/user-events"
        partition-columns: ["year", "month", "day", "event_type"]
        table-name: "user_events"
      processing:
        batch-size: 1000
        flush-interval: 60
        max-retries: 3
    
    order-events:
      kafka-topic: "orders.created.v2"
      schema-file: "schemas/order-events-schema.json"
      destination:
        bucket: "analytics-lake"
        path: "orders/order-events"
        partition-columns: ["year", "month", "day", "region"]
        table-name: "order_events"
      processing:
        batch-size: 500
        flush-interval: 30
        max-retries: 5
    
    system-logs:
      kafka-topic: "system.logs"
      schema-file: "schemas/log-schema.json"
      destination:
        bucket: "logs-lake"
        path: "logs/system-logs"
        partition-columns: ["year", "month", "day", "hour", "log_level"]
        table-name: "system_logs"
      processing:
        batch-size: 2000
        flush-interval: 120
        max-retries: 2

monitoring:
  prometheus-port: 8080
  health-check-port: 8081
```

#### Step 2.2: Configuration Classes
Implement configuration classes for multi-topic support:

```java
@ConfigurationProperties(prefix = "connector")
@Data
public class ConnectorConfiguration {
    private Map<String, TopicConfig> topics = new HashMap<>();
}

@Data
public class TopicConfig {
    @NotBlank
    private String kafkaTopic;
    
    @NotBlank
    private String schemaFile;
    
    @Valid
    @NotNull
    private DestinationConfig destination;
    
    @Valid
    @NotNull
    private ProcessingConfig processing;
}

@Data
public class DestinationConfig {
    @NotBlank
    private String bucket;
    
    @NotBlank
    private String path;
    
    @NotBlank
    private String tableName;
    
    @NotEmpty
    private List<String> partitionColumns;
    
    // Delta Lake specific configurations
    private DeltaConfig deltaConfig = new DeltaConfig();
}

@Data
public class DeltaConfig {
    private boolean enableOptimize = true;
    private int optimizeInterval = 10; // batches
    private boolean enableVacuum = false;
    private int vacuumRetentionHours = 168; // 7 days
    private boolean enableSchemaEvolution = true;
    private String checkpointInterval = "10";
}

@Data
public class ProcessingConfig {
    @Min(1)
    private int batchSize = 1000;
    
    @Min(1)
    private int flushInterval = 60;
    
    @Min(0)
    private int maxRetries = 3;
}
```

### Phase 3: Core Component Implementation

#### Step 3.1: Topic Router Service
**Implementation Focus:**
- Dynamic topic registration based on configuration
- Route messages to appropriate processors based on topic
- Manage multiple `@KafkaListener` instances dynamically
- Centralized message routing and error handling

```java
@Service
public class TopicRouterService {
    private final Map<String, TopicConfig> topicConfigs;
    private final SchemaValidationService schemaValidator;
    private final MessageTransformationService transformer;
    private final MultiDestinationDeltaWriterService deltaWriter;
    
    @EventListener(ContextRefreshedEvent.class)
    public void initializeTopicListeners() {
        // Dynamically create @KafkaListener for each configured topic
    }
    
    public void processMessage(String topicName, ConsumerRecord<String, String> record) {
        TopicConfig config = topicConfigs.get(topicName);
        // Route to appropriate processors
    }
}
```

#### Step 3.2: Multi-Topic Kafka Consumer Service
**Implementation Focus:**
- Dynamic `@KafkaListener` creation for each configured topic
- Topic-specific consumer group management
- Per-topic batch processing with individual configurations
- Topic-aware error handling and retry mechanisms

**Key Features:**
- Topic-specific batch sizes and intervals
- Independent offset management per topic
- Topic-specific dead letter queues
- Graceful shutdown handling for all topics

#### Step 3.3: Multi-Schema Validation Service
**Implementation Focus:**
- Multiple schema management (one per topic)
- Schema caching and hot-reloading
- Topic-aware validation routing
- Schema evolution tracking per topic

```java
@Service
public class SchemaValidationService {
    private final Map<String, JsonSchema> schemaCache = new ConcurrentHashMap<>();
    
    public ValidationResult validateMessage(String topicName, String message) {
        JsonSchema schema = getSchemaForTopic(topicName);
        // Validate against topic-specific schema
    }
    
    private JsonSchema getSchemaForTopic(String topicName) {
        return schemaCache.computeIfAbsent(topicName, this::loadSchema);
    }
}
```

**Validation Strategy:**
- Topic-specific validation rules
- Per-topic strict/lenient mode configuration
- Schema version management per topic
- Validation metrics per topic

#### Step 3.4: Multi-Topic Message Transformation Service
**Implementation Focus:**
- Topic-aware message transformation pipelines
- Per-topic field mapping and enrichment rules
- Topic-specific partitioning strategies
- Dynamic transformation based on topic configuration

```java
@Service
public class MessageTransformationService {
    
    public TransformedMessage transform(String topicName, String rawMessage, TopicConfig config) {
        // Apply topic-specific transformations
        TransformedMessage transformed = applyFieldMappings(rawMessage, config);
        transformed = addPartitionKeys(transformed, config.getDestination().getPartitionColumns());
        transformed = enrichWithMetadata(transformed, topicName);
        return transformed;
    }
    
    private TransformedMessage addPartitionKeys(TransformedMessage message, List<String> partitionColumns) {
        // Generate partition values based on configuration
        for (String column : partitionColumns) {
            message.addPartitionValue(column, calculatePartitionValue(column, message));
        }
        return message;
    }
}
```

**Transformation Features:**
- Topic-specific field mapping configurations
- Per-topic data enrichment rules
- Dynamic partition key generation based on topic config
- Topic-aware format standardization

#### Step 3.5: Multi-Destination Delta Writer Service
**Implementation Focus:**
- Multiple Delta Lake destination management (one per topic)
- Topic-specific batch accumulation and flushing
- Per-destination Delta table operations with ACID guarantees
- Independent transaction management per destination

```java
@Service
public class MultiDestinationDeltaWriterService {
    private final Map<String, DeltaTable> deltaTableCache = new ConcurrentHashMap<>();
    private final Map<String, List<TransformedMessage>> batchBuffers = new ConcurrentHashMap<>();
    
    public void writeMessage(String topicName, TransformedMessage message, TopicConfig config) {
        String destinationKey = buildDestinationKey(topicName, config);
        
        batchBuffers.computeIfAbsent(destinationKey, k -> new ArrayList<>()).add(message);
        
        if (shouldFlushBatch(destinationKey, config)) {
            flushBatch(destinationKey, config);
        }
    }
    
    private void flushBatch(String destinationKey, TopicConfig config) {
        List<TransformedMessage> batch = batchBuffers.get(destinationKey);
        DeltaTable deltaTable = getDeltaTableForDestination(destinationKey, config);
        
        // Write batch to Delta Lake with ACID guarantees
        Dataset<Row> batchDF = convertToDataFrame(batch);
        deltaTable.write().mode(SaveMode.Append).save();
        
        // Handle Delta Lake optimizations
        handleOptimizations(deltaTable, config);
        
        batchBuffers.put(destinationKey, new ArrayList<>());
    }
    
    private void handleOptimizations(DeltaTable deltaTable, TopicConfig config) {
        DeltaConfig deltaConfig = config.getDestination().getDeltaConfig();
        
        if (deltaConfig.isEnableOptimize() && shouldOptimize()) {
            deltaTable.optimize().executeCompaction();
        }
        
        if (deltaConfig.isEnableVacuum() && shouldVacuum()) {
            deltaTable.vacuum(deltaConfig.getVacuumRetentionHours());
        }
    }
}
```

**Writer Features:**
- **ACID Transactions**: Guaranteed consistency for each batch write
- **Schema Evolution**: Automatic schema updates when configured
- **Time Travel**: Access to historical versions of data
- **Optimize Operations**: Automatic file compaction for query performance
- **Vacuum Operations**: Cleanup of old file versions
- **Upsert Support**: Merge operations for data updates
- **Partition Management**: Automatic partitioning based on configuration

### Phase 4: Error Handling and Resilience

#### Step 4.1: Exception Handling Strategy
- Custom exception hierarchy
- Retry mechanisms with exponential backoff
- Circuit breaker pattern implementation
- Dead letter queue processing

#### Step 4.2: Monitoring and Observability
- Health check endpoints
- Custom metrics using Micrometer
- Prometheus metrics exposure
- Structured logging with correlation IDs

### Phase 5: Testing Strategy

#### Step 5.1: Unit Testing
- Service layer testing with mocks
- Schema validation testing
- Data transformation testing
- Configuration validation testing

#### Step 5.2: Integration Testing
- Embedded Kafka for integration tests
- TestContainers for S3 integration
- End-to-end pipeline testing
- Performance testing

### Phase 6: Deployment and Operations

#### Step 6.1: Containerization
- Multi-stage Docker build
- Optimized JVM settings
- Health check configuration
- Environment variable injection

#### Step 6.2: Kubernetes Deployment
- Deployment and Service manifests
- ConfigMap and Secret management
- Resource limits and requests
- Pod disruption budgets

#### Step 6.3: Monitoring Setup
- Prometheus scraping configuration
- Grafana dashboard creation
- Alert rule definitions
- Log aggregation setup

## Implementation Timeline

### Week 1: Foundation
- Project setup and initial configuration
- Basic Kafka consumer implementation
- Core service structure

### Week 2: Core Features
- Schema validation implementation
- Message transformation logic
- Basic Delta writer functionality

### Week 3: Integration
- End-to-end pipeline integration
- Error handling implementation
- Initial testing

### Week 4: Monitoring & Deployment
- Metrics and health checks
- Docker configuration
- Documentation completion

## Key Implementation Considerations

### Performance Optimizations
- Batch processing for improved throughput
- Async processing where applicable
- Connection pooling for S3 operations
- JVM tuning for garbage collection

### Security Considerations
- AWS IAM role-based access
- Kafka SSL/SASL authentication
- Sensitive data encryption
- Network security groups

### Operational Excellence
- Comprehensive logging strategy
- Metric collection and alerting
- Graceful degradation handling
- Disaster recovery procedures

## Multi-Topic Configuration Examples

### Example 1: E-commerce Platform Configuration
```yaml
connector:
  topics:
    # User behavior tracking
    user-clicks:
      kafka-topic: "user.clicks.v1"
      schema-file: "schemas/user-clicks.json"
      destination:
        bucket: "analytics-datalake"
        path: "user-behavior/clicks"
        partition-columns: ["year", "month", "day", "user_segment"]
        table-name: "user_clicks"
        delta-config:
          enable-optimize: true
          optimize-interval: 10
          enable-schema-evolution: true
      processing:
        batch-size: 5000
        flush-interval: 30
        max-retries: 3
    
    # Order processing events
    order-events:
      kafka-topic: "orders.lifecycle.v2"
      schema-file: "schemas/order-lifecycle.json"
      destination:
        bucket: "transactional-datalake"
        path: "orders/lifecycle-events"
        partition-columns: ["year", "month", "day", "order_status", "region"]
        table-name: "order_lifecycle"
        delta-config:
          enable-optimize: true
          enable-vacuum: true
          vacuum-retention-hours: 72
      processing:
        batch-size: 1000
        flush-interval: 60
        max-retries: 5
    
    # Inventory updates
    inventory-changes:
      kafka-topic: "inventory.updates"
      schema-file: "schemas/inventory-updates.json"
      destination:
        bucket: "operational-datalake"
        path: "inventory/updates"
        partition-columns: ["year", "month", "day", "warehouse_id", "product_category"]
        table-name: "inventory_updates"
        delta-config:
          enable-optimize: false  # High-volume append-only, optimize less frequently
          optimize-interval: 50
          enable-schema-evolution: false
      processing:
        batch-size: 2000
        flush-interval: 120
        max-retries: 2
```

### Example 2: IoT Platform Configuration
```yaml
connector:
  topics:
    # Device telemetry
    sensor-data:
      kafka-topic: "iot.sensors.telemetry"
      schema-file: "schemas/sensor-telemetry.json"
      destination:
        bucket: "iot-timeseries-lake"
        path: "telemetry/sensors"
        partition-columns: ["year", "month", "day", "hour", "device_type", "location"]
        table-name: "sensor_telemetry"
      processing:
        batch-size: 10000
        flush-interval: 15
        max-retries: 3
    
    # Device alerts
    device-alerts:
      kafka-topic: "iot.alerts.critical"
      schema-file: "schemas/device-alerts.json"
      destination:
        bucket: "iot-alerts-lake"
        path: "alerts/device-alerts"
        partition-columns: ["year", "month", "day", "severity", "device_type"]
        table-name: "device_alerts"
      processing:
        batch-size: 100
        flush-interval: 5
        max-retries: 5
```

### Schema File Examples

#### User Clicks Schema (`schemas/user-clicks.json`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["user_id", "timestamp", "page_url", "event_type"],
  "properties": {
    "user_id": {
      "type": "string",
      "description": "Unique user identifier"
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "page_url": {
      "type": "string",
      "format": "uri"
    },
    "event_type": {
      "type": "string",
      "enum": ["click", "scroll", "hover", "focus"]
    },
    "user_segment": {
      "type": "string",
      "enum": ["premium", "standard", "trial"]
    },
    "session_id": {
      "type": "string"
    }
  }
}
```

#### Order Lifecycle Schema (`schemas/order-lifecycle.json`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["order_id", "timestamp", "order_status", "region"],
  "properties": {
    "order_id": {
      "type": "string"
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "order_status": {
      "type": "string",
      "enum": ["created", "paid", "shipped", "delivered", "cancelled"]
    },
    "region": {
      "type": "string"
    },
    "customer_id": {
      "type": "string"
    },
    "total_amount": {
      "type": "number",
      "minimum": 0
    },
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "product_id": {"type": "string"},
          "quantity": {"type": "integer", "minimum": 1},
          "price": {"type": "number", "minimum": 0}
        }
      }
    }
  }
}
```

### Implementation Benefits

1. **Flexible Topic Management**: Each topic can have completely different:
   - Schema requirements
   - S3 destinations and paths
   - Partitioning strategies
   - Processing configurations

2. **Independent Scaling**: Topics can be scaled independently based on:
   - Message volume
   - Processing complexity
   - Business criticality

3. **Operational Flexibility**: Different topics can have:
   - Different retry policies
   - Separate monitoring and alerting
   - Independent deployment cycles

4. **Data Organization**: Clear separation of data by:
   - Business domain
   - Data sensitivity
   - Access patterns
   - Retention policies

## Success Criteria

1. **Functionality**: Successfully consume from multiple Kafka topics and write to corresponding S3 Delta destinations
2. **Configuration**: Support dynamic addition/removal of topics without code changes
3. **Performance**: Handle different throughput requirements per topic
4. **Reliability**: 99.9% uptime with topic-specific error handling
5. **Observability**: Per-topic monitoring and alerting setup
6. **Maintainability**: Schema evolution support per topic