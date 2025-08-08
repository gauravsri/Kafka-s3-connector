# Kafka-S3-Delta Lake Connector - User Guide

## Table of Contents

1. [Getting Started](#getting-started)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Schema Management](#schema-management)
5. [Operations](#operations)
6. [Monitoring](#monitoring)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)
9. [Examples](#examples)

## Getting Started

This guide helps you set up and operate the Kafka-S3-Delta Lake Connector in different environments, from local development to production deployments.

### What You'll Need

- **Java 17 or higher** with `JAVA_HOME` configured
- **Maven 3.9+** for building from source
- **Docker & Docker Compose** for local development
- **Kafka cluster** (RedPanda for local development)
- **S3-compatible storage** (MinIO for local development, AWS S3 for production)
- **Basic understanding** of Kafka, S3, and Delta Lake concepts

### Quick Validation

Before starting, verify your environment:

```bash
# Check Java version
java -version  # Should show Java 17+

# Check Maven
mvn -version   # Should show Maven 3.9+

# Check Docker
docker --version && docker-compose --version
```

## Installation

### Option 1: Run Pre-built JAR (Recommended)

```bash
# Download the latest release
wget https://github.com/gauravsri/Kafka-s3-connector/releases/download/v1.0.0/kafka-s3-connector-1.0.0.jar

# Run with default configuration
java -jar kafka-s3-connector-1.0.0.jar
```

### Option 2: Build from Source

```bash
# Clone repository
git clone https://github.com/gauravsri/Kafka-s3-connector.git
cd Kafka-s3-connector

# Build the project
mvn clean package -DskipTests

# Run the built JAR
java -jar target/kafka-s3-connector-1.0.0-SNAPSHOT.jar
```

### Option 3: Docker Setup

```bash
# Clone repository
git clone https://github.com/gauravsri/Kafka-s3-connector.git
cd Kafka-s3-connector

# Start infrastructure and connector
docker-compose up -d

# Check services
docker-compose ps
```

## Configuration

### Environment-Specific Configurations

The connector supports multiple configuration approaches:

#### 1. Application Properties

Create `application-prod.yml` for production:

```yaml
# Production Configuration
spring:
  application:
    name: kafka-s3-delta-connector
  profiles:
    active: prod

# Kafka Configuration
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:kafka1:9092,kafka2:9092,kafka3:9092}
    consumer:
      group-id: kafka-s3-delta-connector-prod
      auto-offset-reset: latest
      enable-auto-commit: false

# AWS S3 Configuration  
aws:
  s3:
    region: ${AWS_REGION:us-east-1}
    access-key-id: ${AWS_ACCESS_KEY_ID}
    secret-access-key: ${AWS_SECRET_ACCESS_KEY}
    endpoint: ${AWS_S3_ENDPOINT:}  # Leave empty for AWS S3

# Delta Lake Topics Configuration
connector:
  topics:
    user-events:
      kafka-topic: "user.events.v1"
      schema-file: "schemas/user-events-schema.json"
      destination:
        bucket: ${S3_BUCKET:production-data-lake}
        path: "events/user-events"
        partition-columns: ["year", "month", "day", "event_type"]
        table-name: "user_events"
        delta-config:
          enable-optimize: true
          optimize-interval: 50
          enable-vacuum: true
          vacuum-retention-hours: 168
          enable-schema-evolution: true
      processing:
        batch-size: 5000
        flush-interval: 30
        max-retries: 5
```

#### 2. Environment Variables

Set these environment variables for production:

```bash
# Required Variables
export KAFKA_BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
export S3_BUCKET="your-data-lake-bucket"

# Optional Variables
export SPRING_PROFILES_ACTIVE="prod"
export LOG_LEVEL="INFO"
export JVM_OPTS="-Xmx4g -Xms2g -XX:+UseG1GC"
```

#### 3. Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-s3-delta-config
data:
  application.yml: |
    spring:
      kafka:
        bootstrap-servers: kafka-cluster-service:9092
    connector:
      topics:
        user-events:
          kafka-topic: "user.events.v1"
          destination:
            bucket: "k8s-data-lake"
            partition-columns: ["year", "month", "day"]
          processing:
            batch-size: 2000
            flush-interval: 60
```

### Topic Configuration Reference

Each topic configuration supports the following options:

```yaml
connector:
  topics:
    <topic-name>:
      # Kafka Settings
      kafka-topic: "actual.kafka.topic.name"        # Required
      schema-file: "schemas/schema.json"             # Required
      
      # Delta Lake Destination
      destination:
        bucket: "bucket-name"                        # Required
        path: "path/to/delta/table"                  # Required
        table-name: "table_name"                     # Optional, defaults to topic name
        partition-columns: ["col1", "col2"]          # Optional partitioning
        
        # Delta Lake Configuration
        delta-config:
          enable-optimize: true                      # Auto-optimize tables
          optimize-interval: 10                      # Optimize every N commits
          enable-vacuum: false                       # Clean old files
          vacuum-retention-hours: 168                # Retention period
          enable-schema-evolution: true              # Allow schema changes
          checkpoint-interval: "10"                  # Checkpoint frequency
      
      # Processing Settings
      processing:
        batch-size: 1000                            # Records per batch
        flush-interval: 60                          # Max seconds before flush
        max-retries: 3                              # Retry attempts
```

## Schema Management

### Creating JSON Schemas

Define schemas in `src/main/resources/schemas/` directory:

**User Events Schema (`schemas/user-events-schema.json`):**

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "title": "User Events Schema",
  "description": "Schema for user interaction events",
  "required": ["user_id", "event_type", "timestamp"],
  "properties": {
    "user_id": {
      "type": "string",
      "description": "Unique user identifier"
    },
    "event_type": {
      "type": "string",
      "enum": ["click", "view", "signup", "login", "logout"],
      "description": "Type of user event"
    },
    "timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "Event timestamp in ISO format"
    },
    "page_url": {
      "type": "string",
      "format": "uri",
      "description": "URL where event occurred"
    },
    "session_id": {
      "type": "string",
      "description": "User session identifier"
    },
    "properties": {
      "type": "object",
      "description": "Additional event properties",
      "additionalProperties": true
    }
  },
  "additionalProperties": false
}
```

**Complex Schema with Nested Objects:**

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "title": "Order Events Schema",
  "required": ["order_id", "customer_id", "timestamp"],
  "properties": {
    "order_id": {"type": "string"},
    "customer_id": {"type": "string"},
    "order_status": {
      "type": "string",
      "enum": ["created", "paid", "shipped", "delivered", "cancelled"]
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "billing_address": {
      "type": "object",
      "properties": {
        "street": {"type": "string"},
        "city": {"type": "string"},
        "country": {"type": "string"},
        "postal_code": {"type": "string"}
      },
      "required": ["city", "country"]
    },
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "product_id": {"type": "string"},
          "quantity": {"type": "integer", "minimum": 1},
          "unit_price": {"type": "number", "minimum": 0},
          "attributes": {
            "type": "object",
            "additionalProperties": true
          }
        },
        "required": ["product_id", "quantity", "unit_price"]
      }
    }
  },
  "additionalProperties": false
}
```

### Schema Evolution

The connector automatically handles schema evolution:

1. **Adding Fields**: New optional fields are added automatically
2. **Field Type Changes**: Compatible type changes (string to text)
3. **Nested Objects**: Complex object structures are preserved
4. **Backward Compatibility**: Old data remains queryable

### Validation Testing

Test your schemas before deployment:

```bash
# Validate schema syntax
cat schemas/user-events-schema.json | jq empty

# Test with sample data
echo '{
  "user_id": "user123",
  "event_type": "click", 
  "timestamp": "2024-01-15T10:30:00Z",
  "properties": {"campaign": "summer_sale"}
}' | jq -s '.[0] as $data | 
  ($data | keys) as $dataKeys |
  # Validation logic here
  $data'
```

## Operations

### Starting the Connector

#### Development Mode

```bash
# Start with debug logging
export LOGGING_LEVEL_ROOT=DEBUG
java -jar kafka-s3-connector-1.0.0.jar

# With custom configuration
java -jar kafka-s3-connector-1.0.0.jar --spring.config.location=file:./my-config.yml

# With specific profile
java -jar kafka-s3-connector-1.0.0.jar --spring.profiles.active=staging
```

#### Production Mode

```bash
# Production startup script
export SPRING_PROFILES_ACTIVE=prod
export JVM_OPTS="-Xmx8g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

java $JVM_OPTS -jar kafka-s3-connector-1.0.0.jar \
  --logging.level.com.company.kafkaconnector=INFO \
  --management.endpoints.web.exposure.include=health,metrics,prometheus
```

#### Docker Production

```bash
# Run with production configuration
docker run -d \
  --name kafka-s3-connector \
  -p 8080:8080 \
  -p 8081:8081 \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092 \
  -e AWS_ACCESS_KEY_ID=your-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret \
  -e S3_BUCKET=your-bucket \
  -v /path/to/schemas:/app/schemas:ro \
  kafka-s3-delta-connector:1.0.0
```

### Managing the Service

#### Health Checks

```bash
# Overall health
curl http://localhost:8081/actuator/health

# Detailed component health
curl http://localhost:8081/actuator/health | jq '{
  status: .status,
  kafka: .components.kafka.status,
  s3: .components.s3.status,
  delta: .components.delta.status
}'
```

#### Metrics Monitoring

```bash
# Key performance metrics
curl -s http://localhost:8081/actuator/metrics/kafka.connector.records.processed.total
curl -s http://localhost:8081/actuator/metrics/kafka.connector.batch.flush.duration

# JVM metrics
curl -s http://localhost:8081/actuator/metrics/jvm.memory.used
curl -s http://localhost:8081/actuator/metrics/jvm.gc.pause
```

#### Graceful Shutdown

```bash
# Send SIGTERM for graceful shutdown
kill -TERM <pid>

# Or use actuator endpoint
curl -X POST http://localhost:8081/actuator/shutdown
```

### Data Verification

#### Verify Delta Tables

```bash
# List Delta tables in S3
aws s3 ls s3://your-bucket/events/ --recursive

# Check Delta transaction log
aws s3 cp s3://your-bucket/events/user-events/_delta_log/00000000000000000000.json - | jq .

# Verify Parquet files
aws s3 ls s3://your-bucket/events/user-events/ --recursive | grep -E '\.parquet$'
```

#### Query Delta Tables

Using SQL engines that support Delta Lake:

```sql
-- Query user events
SELECT 
    event_type,
    COUNT(*) as event_count,
    DATE(timestamp) as event_date
FROM delta.`s3a://your-bucket/events/user-events/`
WHERE DATE(timestamp) = '2024-01-15'
GROUP BY event_type, DATE(timestamp)
ORDER BY event_count DESC;

-- Time travel query
SELECT * FROM delta.`s3a://your-bucket/events/user-events/`
VERSION AS OF 1;
```

## Monitoring

### Setting Up Monitoring

#### Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'kafka-s3-connector'
    static_configs:
      - targets: ['localhost:8081']
    metrics_path: '/actuator/prometheus'
    scrape_interval: 30s
```

#### Grafana Dashboard

Import the provided dashboard or create custom panels:

**Key Panels:**
- Records processed per second
- Batch processing latency
- Schema validation failures
- Delta table sizes
- JVM memory and GC metrics

#### Alerting Rules

```yaml
# alerts.yml
groups:
- name: kafka-s3-connector
  rules:
  - alert: HighProcessingLatency
    expr: kafka_connector_batch_flush_duration_seconds{quantile="0.95"} > 30
    for: 5m
    annotations:
      summary: "High processing latency detected"
      
  - alert: SchemaValidationFailures
    expr: rate(kafka_connector_schema_validation_failures_total[5m]) > 0.1
    for: 2m
    annotations:
      summary: "High schema validation failure rate"
      
  - alert: ConnectorDown
    expr: up{job="kafka-s3-connector"} == 0
    for: 1m
    annotations:
      summary: "Kafka S3 Connector is down"
```

### Log Analysis

#### Structured Logging

The connector produces structured JSON logs:

```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "level": "INFO",
  "logger": "DeltaWriterService", 
  "thread": "kafka-consumer-1",
  "correlation_id": "abc-123-def",
  "topic": "user.events.v1",
  "message": "Successfully wrote batch to Delta Lake",
  "batch_size": 1000,
  "processing_time_ms": 250,
  "delta_version": 15,
  "files_written": 3
}
```

#### Log Queries

```bash
# Find processing errors
grep -i error logs/kafka-s3-connector.log | jq .

# Monitor batch processing
tail -f logs/kafka-s3-connector.log | grep "Successfully wrote batch" | jq '{timestamp, topic, batch_size, processing_time_ms}'

# Schema validation issues
tail -f logs/kafka-s3-connector.log | grep "Schema validation" | jq .
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Connection Issues

**Problem**: Cannot connect to Kafka
```bash
# Check Kafka connectivity
telnet kafka-broker 9092

# Verify topic exists
kafka-topics --bootstrap-server kafka-broker:9092 --list | grep user.events.v1

# Test consumer group
kafka-consumer-groups --bootstrap-server kafka-broker:9092 --group kafka-s3-delta-connector --describe
```

**Problem**: S3 connection failures
```bash
# Test S3 connectivity
aws s3 ls s3://your-bucket/ --endpoint-url http://your-endpoint

# Check credentials
aws configure list

# Verify bucket permissions
aws s3api get-bucket-policy --bucket your-bucket
```

#### 2. Schema Validation Errors

**Problem**: Messages fail schema validation

```bash
# Check schema file
cat schemas/user-events-schema.json | jq .

# Test with sample message
echo '{"user_id":"test","event_type":"invalid"}' | 
  ajv validate -s schemas/user-events-schema.json -d /dev/stdin
```

**Solution**: Update schema or fix message format:
- Ensure required fields are present
- Check field types match schema definitions
- Verify enum values are valid

#### 3. Performance Issues

**Problem**: High processing latency

```bash
# Check batch sizes
curl -s http://localhost:8081/actuator/metrics/kafka.connector.batch.size | jq .

# Monitor memory usage
curl -s http://localhost:8081/actuator/metrics/jvm.memory.used | jq .

# Check GC performance
curl -s http://localhost:8081/actuator/metrics/jvm.gc.pause | jq .
```

**Solutions**:
- Increase batch size for better throughput
- Tune JVM memory settings
- Optimize schema complexity
- Check S3 write performance

#### 4. Delta Lake Issues

**Problem**: Delta write failures

```bash
# Check Delta transaction logs
aws s3 ls s3://your-bucket/table-path/_delta_log/ | tail -5

# Verify Parquet files
aws s3 ls s3://your-bucket/table-path/ | grep -E '\.parquet$' | wc -l
```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
# Enable debug for connector
export LOGGING_LEVEL_COM_COMPANY_KAFKACONNECTOR=DEBUG

# Enable debug for specific component
export LOGGING_LEVEL_COM_COMPANY_KAFKACONNECTOR_SERVICE_DELTAWRITERSERVICE=TRACE

# Enable Spring Kafka debug
export LOGGING_LEVEL_ORG_SPRINGFRAMEWORK_KAFKA=DEBUG
```

## Best Practices

### Configuration Best Practices

1. **Environment-specific configs**: Use profiles for different environments
2. **Secret management**: Store sensitive data in secure vaults
3. **Resource limits**: Set appropriate CPU and memory limits
4. **Batch tuning**: Optimize batch size based on message size and throughput needs
5. **Monitoring**: Always enable comprehensive monitoring

### Schema Design Best Practices

1. **Version schemas**: Use versioned schema files
2. **Required fields**: Minimize required fields for flexibility
3. **Field naming**: Use consistent, descriptive field names
4. **Documentation**: Add descriptions to all fields
5. **Evolution planning**: Design schemas with future evolution in mind

### Operations Best Practices

1. **Health checks**: Implement comprehensive health checks
2. **Graceful shutdown**: Always use graceful shutdown procedures
3. **Resource monitoring**: Monitor CPU, memory, and I/O continuously
4. **Log retention**: Configure appropriate log retention policies
5. **Backup strategies**: Implement backup and disaster recovery plans

### Performance Best Practices

1. **Batch optimization**: Balance batch size and processing latency
2. **Partitioning**: Use effective partitioning strategies
3. **Compression**: Enable compression for Parquet files
4. **Connection pooling**: Reuse S3 connections efficiently
5. **JVM tuning**: Optimize JVM settings for your workload

## Examples

### Complete Working Example

This example shows a complete setup for processing e-commerce events:

#### 1. Schema Definition

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "title": "E-commerce Events Schema",
  "required": ["event_id", "user_id", "event_type", "timestamp"],
  "properties": {
    "event_id": {"type": "string"},
    "user_id": {"type": "string"}, 
    "event_type": {
      "type": "string",
      "enum": ["page_view", "product_view", "add_to_cart", "purchase", "search"]
    },
    "timestamp": {"type": "string", "format": "date-time"},
    "session_id": {"type": "string"},
    "page_url": {"type": "string", "format": "uri"},
    "referrer": {"type": "string"},
    "user_agent": {"type": "string"},
    "ip_address": {"type": "string", "format": "ipv4"},
    "product": {
      "type": "object",
      "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "category": {"type": "string"},
        "price": {"type": "number", "minimum": 0},
        "brand": {"type": "string"},
        "attributes": {
          "type": "object",
          "additionalProperties": true
        }
      }
    },
    "cart": {
      "type": "object", 
      "properties": {
        "total_value": {"type": "number", "minimum": 0},
        "item_count": {"type": "integer", "minimum": 0},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"}
      }
    },
    "custom_attributes": {
      "type": "object",
      "additionalProperties": true
    }
  },
  "additionalProperties": false
}
```

#### 2. Configuration

```yaml
connector:
  topics:
    ecommerce-events:
      kafka-topic: "ecommerce.events.v1"
      schema-file: "schemas/ecommerce-events-schema.json"
      destination:
        bucket: "analytics-data-lake"
        path: "events/ecommerce"
        partition-columns: ["year", "month", "day", "event_type"]
        table-name: "ecommerce_events"
        delta-config:
          enable-optimize: true
          optimize-interval: 20
          enable-vacuum: true
          vacuum-retention-hours: 720  # 30 days
          enable-schema-evolution: true
      processing:
        batch-size: 2000
        flush-interval: 45
        max-retries: 5
```

#### 3. Sample Data

```bash
# Publish sample events
cat << 'EOF' | kafka-console-producer --bootstrap-server localhost:9092 --topic ecommerce.events.v1
{
  "event_id": "evt_001",
  "user_id": "usr_12345", 
  "event_type": "product_view",
  "timestamp": "2024-01-15T14:30:15Z",
  "session_id": "sess_abc123",
  "page_url": "https://shop.example.com/products/laptop-pro",
  "referrer": "https://google.com",
  "user_agent": "Mozilla/5.0...",
  "ip_address": "192.168.1.100",
  "product": {
    "id": "prod_laptop_001",
    "name": "Professional Laptop Pro", 
    "category": "Electronics/Computers",
    "price": 1299.99,
    "brand": "TechCorp",
    "attributes": {
      "color": "Silver",
      "memory": "16GB",
      "storage": "512GB SSD"
    }
  },
  "custom_attributes": {
    "campaign": "summer_sale_2024",
    "discount_code": "SAVE20"
  }
}
EOF
```

#### 4. Monitoring Setup

```bash
# Create Grafana dashboard query
{
  "targets": [
    {
      "expr": "rate(kafka_connector_records_processed_total{topic=\"ecommerce.events.v1\"}[5m])",
      "legendFormat": "Records/sec"
    },
    {
      "expr": "kafka_connector_batch_flush_duration_seconds{quantile=\"0.95\",topic=\"ecommerce.events.v1\"}",
      "legendFormat": "95th Percentile Latency"
    }
  ]
}
```

#### 5. Data Verification

```bash
# Check Delta table structure
aws s3 ls s3://analytics-data-lake/events/ecommerce/_delta_log/ | head -5

# Query with Spark/Delta
spark-sql --packages io.delta:delta-core_2.12:2.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  -e "
    SELECT 
      event_type,
      COUNT(*) as events,
      AVG(product.price) as avg_price
    FROM delta.\`s3a://analytics-data-lake/events/ecommerce/\`
    WHERE DATE(timestamp) = '2024-01-15'
    GROUP BY event_type
  "
```

This user guide provides comprehensive coverage of all aspects needed to successfully deploy and operate the Kafka-S3-Delta Lake Connector in any environment.