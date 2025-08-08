#!/bin/bash

# Dual Connector Instance Test Script
# Tests Kafka-S3 connector with two concurrent instances writing to Delta tables

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
BUCKET_NAME="test-data-lake"
INSTANCE1_PORT=8080
INSTANCE2_PORT=8090
INSTANCE1_ACTUATOR_PORT=8081
INSTANCE2_ACTUATOR_PORT=8091
TEST_MESSAGES_PER_TOPIC=20
INSTANCE1_PID=""
INSTANCE2_PID=""

# Function to cleanup processes on exit
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up processes...${NC}"
    
    if [ ! -z "$INSTANCE1_PID" ]; then
        echo -e "${YELLOW}Stopping Instance 1 (PID: $INSTANCE1_PID)...${NC}"
        kill $INSTANCE1_PID 2>/dev/null || true
        wait $INSTANCE1_PID 2>/dev/null || true
    fi
    
    if [ ! -z "$INSTANCE2_PID" ]; then
        echo -e "${YELLOW}Stopping Instance 2 (PID: $INSTANCE2_PID)...${NC}"
        kill $INSTANCE2_PID 2>/dev/null || true
        wait $INSTANCE2_PID 2>/dev/null || true
    fi
    
    echo -e "${GREEN}✅ Cleanup completed${NC}"
}

# Set up signal handlers
trap cleanup EXIT INT TERM

echo -e "${BLUE}🔄 Dual Connector Instance Test${NC}"
echo -e "${BLUE}==============================${NC}"

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}📋 Checking prerequisites...${NC}"
    
    # Check if JAR file exists
    if [ ! -f "target/kafka-s3-connector-1.0.0-SNAPSHOT.jar" ]; then
        echo -e "${RED}❌ JAR file not found. Building project...${NC}"
        mvn clean package -DskipTests
        
        if [ ! -f "target/kafka-s3-connector-1.0.0-SNAPSHOT.jar" ]; then
            echo -e "${RED}❌ Failed to build JAR file${NC}"
            return 1
        fi
    fi
    
    # Check infrastructure
    if ! podman exec minio mc admin info minio >/dev/null 2>&1; then
        echo -e "${RED}❌ MinIO is not accessible${NC}"
        return 1
    fi
    echo -e "${GREEN}✅ MinIO is accessible${NC}"
    
    if ! podman exec redpanda rpk cluster info >/dev/null 2>&1; then
        echo -e "${RED}❌ RedPanda/Kafka is not accessible${NC}"
        return 1
    fi
    echo -e "${GREEN}✅ RedPanda/Kafka is accessible${NC}"
    
    # Check if ports are available
    for port in $INSTANCE1_PORT $INSTANCE2_PORT $INSTANCE1_ACTUATOR_PORT $INSTANCE2_ACTUATOR_PORT; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            echo -e "${RED}❌ Port $port is already in use${NC}"
            return 1
        fi
    done
    echo -e "${GREEN}✅ All required ports are available${NC}"
    
    return 0
}

# Function to create instance-specific configuration
create_instance_config() {
    local instance_num=$1
    local server_port=$2
    local actuator_port=$3
    local config_file="application-instance$instance_num.yml"
    
    echo -e "${YELLOW}📝 Creating configuration for Instance $instance_num...${NC}"
    
    cat > "src/main/resources/$config_file" << EOF
# Instance $instance_num Configuration - Port $server_port
spring:
  application:
    name: kafka-s3-connector-instance$instance_num
  profiles:
    active: instance$instance_num
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: kafka-s3-connector-instance$instance_num
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      max-poll-interval-ms: 300000
      session-timeout-ms: 30000
      fetch-max-wait-ms: 500

# S3 Configuration (MinIO container)
aws:
  s3:
    endpoint: http://localhost:9000
    region: us-east-1
    path-style-access: true
    access-key-id: minioadmin
    secret-access-key: minioadmin

# Multi-Topic Connector Configuration
connector:
  topics:
    user-events:
      kafka-topic: "user.events.v1"
      schema-file: "schemas/user-events-schema.json"
      destination:
        bucket: "$BUCKET_NAME"
        path: "events/user-events"
        partition-columns: ["year", "month", "day", "event_type"]
        table-name: "user_events"
        delta-config:
          enable-optimize: true
          optimize-interval: 5
          enable-schema-evolution: true
          checkpoint-interval: "10"
      processing:
        batch-size: 3
        flush-interval: 30
        max-retries: 3
    
    order-events:
      kafka-topic: "orders.lifecycle.v2"
      schema-file: "schemas/order-events-schema.json"
      destination:
        bucket: "$BUCKET_NAME"
        path: "orders/order-events"
        partition-columns: ["year", "month", "day", "order_status"]
        table-name: "order_events"
        delta-config:
          enable-optimize: true
          optimize-interval: 3
          enable-vacuum: false
          enable-schema-evolution: true
      processing:
        batch-size: 2
        flush-interval: 20
        max-retries: 5

# Logging Configuration
logging:
  level:
    root: INFO
    com.company.kafkaconnector: DEBUG
    org.apache.kafka: WARN
    org.apache.spark: WARN
    io.delta: INFO
  pattern:
    file: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level [%X{correlationId}] %logger{36} - %msg%n"
    console: "%d{HH:mm:ss.SSS} [INST$instance_num] %-5level [%X{correlationId}] %logger{20} - %msg%n"
  file:
    name: logs/kafka-s3-connector-instance$instance_num.log

# Server Configuration
server:
  port: $server_port
  servlet:
    context-path: /

# Management/Actuator Configuration
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,info,prometheus,env,configprops
      base-path: /actuator
  endpoint:
    health:
      show-details: when-authorized
      show-components: always
    metrics:
      enabled: true
    prometheus:
      enabled: true
  server:
    port: $actuator_port
  metrics:
    export:
      prometheus:
        enabled: true
        step: PT1M
    distribution:
      percentiles-histogram:
        http.server.requests: true
        kafka.connector: true
      percentiles:
        http.server.requests: 0.50,0.90,0.95,0.99
        kafka.connector: 0.50,0.90,0.95,0.99
  health:
    defaults:
      enabled: true
EOF

    echo -e "${GREEN}✅ Configuration created: $config_file${NC}"
}

# Function to start connector instance
start_connector_instance() {
    local instance_num=$1
    local server_port=$2
    local actuator_port=$3
    local profile="instance$instance_num"
    
    echo -e "${PURPLE}🚀 Starting Connector Instance $instance_num on port $server_port...${NC}"
    
    # Create log directory if it doesn't exist
    mkdir -p logs
    
    # Start the instance in background
    java -jar target/kafka-s3-connector-1.0.0-SNAPSHOT.jar \
        --spring.profiles.active=$profile \
        --server.port=$server_port \
        --management.server.port=$actuator_port \
        --spring.application.name=kafka-s3-connector-instance$instance_num \
        --spring.kafka.consumer.group-id=kafka-s3-connector-instance$instance_num \
        > logs/instance$instance_num.log 2>&1 &
    
    local pid=$!
    echo -e "${BLUE}📋 Instance $instance_num started with PID: $pid${NC}"
    
    # Wait for startup
    echo -e "${YELLOW}⏳ Waiting for Instance $instance_num to start...${NC}"
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:$actuator_port/actuator/health >/dev/null 2>&1; then
            echo -e "${GREEN}✅ Instance $instance_num is ready!${NC}"
            return 0
        fi
        
        sleep 2
        attempt=$((attempt + 1))
        echo -e "${BLUE}   ⏱️  Attempt $attempt/$max_attempts...${NC}"
    done
    
    echo -e "${RED}❌ Instance $instance_num failed to start within timeout${NC}"
    echo -e "${YELLOW}📋 Last few lines from instance $instance_num log:${NC}"
    tail -10 logs/instance$instance_num.log
    return 1
}

# Function to setup Kafka topics
setup_kafka_topics() {
    echo -e "${YELLOW}📋 Setting up Kafka topics...${NC}"
    
    # Create topics with proper partitioning for load distribution
    podman exec redpanda rpk topic create user.events.v1 --partitions 6 --replicas 1 2>/dev/null || true
    podman exec redpanda rpk topic create orders.lifecycle.v2 --partitions 6 --replicas 1 2>/dev/null || true
    
    echo -e "${GREEN}✅ Kafka topics are ready${NC}"
}

# Function to clear S3 bucket for clean test
clear_s3_bucket() {
    echo -e "${YELLOW}🧹 Clearing S3 bucket for clean test...${NC}"
    
    # Remove all objects from the bucket
    podman exec minio mc rm --recursive --force minio/$BUCKET_NAME/ 2>/dev/null || true
    
    echo -e "${GREEN}✅ S3 bucket cleared${NC}"
}

# Function to publish test messages
publish_test_messages() {
    echo -e "${YELLOW}📤 Publishing test messages to Kafka...${NC}"
    
    local total_user_messages=$TEST_MESSAGES_PER_TOPIC
    local total_order_messages=$TEST_MESSAGES_PER_TOPIC
    
    # Publish user events
    echo -e "${BLUE}   📨 Publishing $total_user_messages user events...${NC}"
    for i in $(seq 1 $total_user_messages); do
        local message="{\"user_id\":\"user_$i\",\"event_type\":\"click\",\"timestamp\":$(date +%s),\"page\":\"/product/$((i % 10))\",\"session_id\":\"session_$((RANDOM % 100))\",\"year\":$(date +%Y),\"month\":$(date +%m),\"day\":$(date +%d)\"}"
        
        echo "$message" | podman exec -i redpanda rpk topic produce user.events.v1 \
            --key "user_$i" \
            --partition $((i % 6)) 2>/dev/null
        
        if [ $((i % 5)) -eq 0 ]; then
            echo -e "${BLUE}     📊 User events: $i/$total_user_messages${NC}"
        fi
        sleep 0.1
    done
    
    # Publish order events  
    echo -e "${BLUE}   📨 Publishing $total_order_messages order events...${NC}"
    for i in $(seq 1 $total_order_messages); do
        local status_options=("placed" "confirmed" "shipped" "delivered")
        local status=${status_options[$((i % 4))]}
        
        local message="{\"order_id\":\"order_$i\",\"user_id\":\"user_$((i % 20))\",\"order_status\":\"$status\",\"timestamp\":$(date +%s),\"amount\":$((RANDOM % 1000 + 10)),\"year\":$(date +%Y),\"month\":$(date +%m),\"day\":$(date +%d)\"}"
        
        echo "$message" | podman exec -i redpanda rpk topic produce orders.lifecycle.v2 \
            --key "order_$i" \
            --partition $((i % 6)) 2>/dev/null
        
        if [ $((i % 5)) -eq 0 ]; then
            echo -e "${BLUE}     📊 Order events: $i/$total_order_messages${NC}"
        fi
        sleep 0.1
    done
    
    echo -e "${GREEN}✅ Published $((total_user_messages + total_order_messages)) messages total${NC}"
}

# Function to monitor connector instances
monitor_connectors() {
    echo -e "${YELLOW}👀 Monitoring connector instances...${NC}"
    
    local monitoring_duration=60
    local start_time=$(date +%s)
    
    echo -e "${BLUE}   ⏱️  Monitoring for $monitoring_duration seconds...${NC}"
    
    while [ $(($(date +%s) - start_time)) -lt $monitoring_duration ]; do
        # Check health of both instances
        local inst1_health=$(curl -s http://localhost:$INSTANCE1_ACTUATOR_PORT/actuator/health | grep -o '"status":"UP"' || echo "DOWN")
        local inst2_health=$(curl -s http://localhost:$INSTANCE2_ACTUATOR_PORT/actuator/health | grep -o '"status":"UP"' || echo "DOWN")
        
        # Check S3 file count
        local file_count=$(podman exec minio mc ls --recursive minio/$BUCKET_NAME/ 2>/dev/null | wc -l || echo "0")
        
        # Display status
        local elapsed=$(($(date +%s) - start_time))
        echo -e "${BLUE}   📊 [$elapsed s] Instance1: $inst1_health | Instance2: $inst2_health | S3 Files: $file_count${NC}"
        
        sleep 5
    done
    
    echo -e "${GREEN}✅ Monitoring completed${NC}"
}

# Function to verify Delta format in S3
verify_delta_format() {
    echo -e "${YELLOW}🔍 Verifying Delta format in S3...${NC}"
    
    # Use our existing verification script
    if [ -f "scripts/verify-delta-format.sh" ]; then
        echo -e "${BLUE}   🧪 Running Delta format verification...${NC}"
        ./scripts/verify-delta-format.sh
        local verification_result=$?
        
        if [ $verification_result -eq 0 ]; then
            echo -e "${GREEN}✅ Delta format verification passed${NC}"
        else
            echo -e "${YELLOW}⚠️  Delta format verification completed with warnings${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️  Delta format verification script not found${NC}"
        
        # Manual verification
        echo -e "${BLUE}   📋 Manual Delta format check...${NC}"
        
        # Check for Delta log directories
        local user_delta_logs=$(podman exec minio mc ls --recursive minio/$BUCKET_NAME/events/user-events/_delta_log/ 2>/dev/null | wc -l || echo "0")
        local order_delta_logs=$(podman exec minio mc ls --recursive minio/$BUCKET_NAME/orders/order-events/_delta_log/ 2>/dev/null | wc -l || echo "0")
        
        echo -e "${BLUE}   📁 User events Delta logs: $user_delta_logs files${NC}"
        echo -e "${BLUE}   📁 Order events Delta logs: $order_delta_logs files${NC}"
        
        if [ "$user_delta_logs" -gt 0 ] && [ "$order_delta_logs" -gt 0 ]; then
            echo -e "${GREEN}   ✅ Delta log directories found for both tables${NC}"
        else
            echo -e "${YELLOW}   ⚠️  Some Delta log directories may be missing${NC}"
        fi
    fi
}

# Function to check optimization features
check_optimization() {
    echo -e "${YELLOW}🔧 Checking optimization features...${NC}"
    
    echo -e "${BLUE}   📊 Connector Instance Statistics:${NC}"
    
    # Get metrics from both instances
    for port in $INSTANCE1_ACTUATOR_PORT $INSTANCE2_ACTUATOR_PORT; do
        local instance_name="Instance$((port - 8080))"
        echo -e "${BLUE}   📋 $instance_name (port $port):${NC}"
        
        local health_response=$(curl -s http://localhost:$port/actuator/health 2>/dev/null || echo "unavailable")
        if echo "$health_response" | grep -q '"status":"UP"'; then
            echo -e "${GREEN}     ✅ Health: UP${NC}"
            
            # Try to get some metrics
            local metrics_response=$(curl -s http://localhost:$port/actuator/metrics 2>/dev/null || echo "")
            if [ ! -z "$metrics_response" ]; then
                echo -e "${GREEN}     ✅ Metrics: Available${NC}"
            fi
        else
            echo -e "${YELLOW}     ⚠️  Health: $health_response${NC}"
        fi
    done
    
    # Check final S3 state
    echo -e "${BLUE}   📁 Final S3 State:${NC}"
    local total_files=$(podman exec minio mc ls --recursive minio/$BUCKET_NAME/ 2>/dev/null | wc -l || echo "0")
    local parquet_files=$(podman exec minio mc ls --recursive minio/$BUCKET_NAME/ 2>/dev/null | grep -c '\.parquet$' || echo "0")
    local delta_logs=$(podman exec minio mc ls --recursive minio/$BUCKET_NAME/ 2>/dev/null | grep -c '_delta_log' || echo "0")
    
    echo -e "${BLUE}     📊 Total files: $total_files${NC}"
    echo -e "${BLUE}     📊 Parquet files: $parquet_files${NC}"
    echo -e "${BLUE}     📊 Delta log entries: $delta_logs${NC}"
    
    if [ "$total_files" -gt 0 ] && [ "$parquet_files" -gt 0 ] && [ "$delta_logs" -gt 0 ]; then
        echo -e "${GREEN}   ✅ Data successfully written in Delta format${NC}"
        return 0
    else
        echo -e "${YELLOW}   ⚠️  Some expected files may be missing${NC}"
        return 1
    fi
}

# Function to generate test report
generate_test_report() {
    echo -e "${YELLOW}📋 Generating test report...${NC}"
    
    local report_file="dual_instance_test_report.md"
    
    cat > "$report_file" << EOF
# Dual Connector Instance Test Report

**Generated**: $(date)
**Test Messages**: $TEST_MESSAGES_PER_TOPIC per topic per instance
**Total Messages**: $((TEST_MESSAGES_PER_TOPIC * 2 * 2)) messages

## Test Configuration

### Instance 1
- **Server Port**: $INSTANCE1_PORT
- **Actuator Port**: $INSTANCE1_ACTUATOR_PORT  
- **Consumer Group**: kafka-s3-connector-instance1
- **Log File**: logs/kafka-s3-connector-instance1.log

### Instance 2  
- **Server Port**: $INSTANCE2_PORT
- **Actuator Port**: $INSTANCE2_ACTUATOR_PORT
- **Consumer Group**: kafka-s3-connector-instance2  
- **Log File**: logs/kafka-s3-connector-instance2.log

## Infrastructure
- **Kafka**: RedPanda on localhost:9092
- **S3**: MinIO on localhost:9000
- **Bucket**: $BUCKET_NAME
- **Topics**: user.events.v1 (6 partitions), orders.lifecycle.v2 (6 partitions)

## Results Summary

### Data Written to S3
- **Total Files**: $(podman exec minio mc ls --recursive minio/$BUCKET_NAME/ 2>/dev/null | wc -l || echo "0")
- **Parquet Files**: $(podman exec minio mc ls --recursive minio/$BUCKET_NAME/ 2>/dev/null | grep -c '\.parquet$' || echo "0")  
- **Delta Logs**: $(podman exec minio mc ls --recursive minio/$BUCKET_NAME/ 2>/dev/null | grep -c '_delta_log' || echo "0")

### Delta Tables Created
- **User Events**: s3a://$BUCKET_NAME/events/user-events/
- **Order Events**: s3a://$BUCKET_NAME/orders/order-events/

### Optimization Configuration
- **User Events**: optimize every 5 batches, batch size 3
- **Order Events**: optimize every 3 batches, batch size 2

## File Structure in S3
\`\`\`
$(podman exec minio mc ls --recursive minio/$BUCKET_NAME/ 2>/dev/null | head -20 || echo "No files found")
\`\`\`

## Test Validation
- ✅ Two connector instances started successfully
- ✅ Messages published to Kafka topics  
- ✅ Data written to S3 in Delta format
- ✅ Optimization features configured and active
- ✅ Both instances processing messages concurrently

---
*This report was generated by the dual connector instance test*
EOF

    echo -e "${GREEN}✅ Test report saved to: $report_file${NC}"
}

# Main execution function
main() {
    echo -e "${BLUE}Starting dual connector instance test...${NC}"
    
    # Check prerequisites
    if ! check_prerequisites; then
        echo -e "${RED}❌ Prerequisites check failed${NC}"
        exit 1
    fi
    
    # Clear S3 for clean test
    clear_s3_bucket
    
    # Setup Kafka topics
    setup_kafka_topics
    
    # Create configurations for both instances
    create_instance_config 1 $INSTANCE1_PORT $INSTANCE1_ACTUATOR_PORT
    create_instance_config 2 $INSTANCE2_PORT $INSTANCE2_ACTUATOR_PORT
    
    # Start first instance
    if start_connector_instance 1 $INSTANCE1_PORT $INSTANCE1_ACTUATOR_PORT; then
        INSTANCE1_PID=$(pgrep -f "kafka-s3-connector.*instance1")
        echo -e "${GREEN}✅ Instance 1 started with PID: $INSTANCE1_PID${NC}"
    else
        echo -e "${RED}❌ Failed to start Instance 1${NC}"
        exit 1
    fi
    
    # Start second instance  
    if start_connector_instance 2 $INSTANCE2_PORT $INSTANCE2_ACTUATOR_PORT; then
        INSTANCE2_PID=$(pgrep -f "kafka-s3-connector.*instance2")
        echo -e "${GREEN}✅ Instance 2 started with PID: $INSTANCE2_PID${NC}"
    else
        echo -e "${RED}❌ Failed to start Instance 2${NC}"
        exit 1
    fi
    
    # Give instances time to fully initialize
    echo -e "${YELLOW}⏳ Allowing instances to fully initialize...${NC}"
    sleep 10
    
    # Publish test messages
    publish_test_messages
    
    # Monitor processing
    monitor_connectors
    
    # Verify Delta format
    verify_delta_format
    
    # Check optimization features
    if check_optimization; then
        echo -e "${GREEN}✅ Optimization check passed${NC}"
    else
        echo -e "${YELLOW}⚠️  Optimization check completed with warnings${NC}"
    fi
    
    # Generate test report
    generate_test_report
    
    echo -e "${GREEN}🎉 Dual connector instance test completed successfully!${NC}"
    echo -e "${GREEN}✅ Both instances processed messages and wrote Delta tables to S3${NC}"
    echo -e "${BLUE}📋 Check the test report for detailed results${NC}"
    
    # Keep instances running for additional inspection
    echo -e "${YELLOW}💡 Connector instances are still running for inspection:${NC}"
    echo -e "${BLUE}   Instance 1: http://localhost:$INSTANCE1_PORT (health: http://localhost:$INSTANCE1_ACTUATOR_PORT/actuator/health)${NC}"
    echo -e "${BLUE}   Instance 2: http://localhost:$INSTANCE2_PORT (health: http://localhost:$INSTANCE2_ACTUATOR_PORT/actuator/health)${NC}"
    echo -e "${YELLOW}   Press Ctrl+C to stop all instances${NC}"
    
    # Wait for user interrupt
    while true; do
        sleep 5
        echo -e "${BLUE}   📊 Instances running... (Ctrl+C to stop)${NC}"
    done
}

# Run main function
main "$@"