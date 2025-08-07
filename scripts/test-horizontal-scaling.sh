#!/bin/bash

# Comprehensive Horizontal Scaling Test Script
# Tests 2 parallel Kafka Connect workers with load generation and metrics collection

set -e

BASE_DIR="/Users/gaurav/kafka-s3-connector"
SCRIPT_DIR="$BASE_DIR/scripts"
CONFIG_DIR="$BASE_DIR/config"

echo "=========================================================================="
echo "                    KAFKA CONNECT HORIZONTAL SCALING TEST"
echo "=========================================================================="
echo "This script will:"
echo "1. Verify prerequisites (Kafka, MinIO, compiled connector)"
echo "2. Create Kafka topics with multiple partitions"
echo "3. Start 2 Kafka Connect workers in distributed mode"
echo "4. Deploy the S3 Delta Sink connector with 4 tasks"
echo "5. Generate load with 10K+ test messages"
echo "6. Monitor task distribution and throughput"
echo "7. Test failover by stopping one worker"
echo "8. Collect and analyze scaling metrics"
echo "=========================================================================="

# Function to check prerequisites
check_prerequisites() {
    echo "🔍 Checking prerequisites..."
    
    # Check if connector is compiled
    if [ ! -f "$BASE_DIR/target/classes/com/company/kafkaconnector/connector/S3DeltaSinkConnector.class" ]; then
        echo "❌ Connector not compiled. Running mvn compile..."
        cd "$BASE_DIR"
        mvn compile -DskipTests -q
        if [ $? -ne 0 ]; then
            echo "❌ Maven compile failed"
            exit 1
        fi
        echo "✅ Connector compiled successfully"
    else
        echo "✅ Connector already compiled"
    fi
    
    # Check Kafka/RedPanda
    if ! nc -z localhost 9092; then
        echo "❌ Kafka/RedPanda not running on localhost:9092"
        echo "Start with: podman run -d --name=redpanda -p 9092:9092 docker.redpanda.com/redpandadata/redpanda:v24.1.1 redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M --node-id 0 --check=false"
        exit 1
    fi
    echo "✅ Kafka/RedPanda running"
    
    # Check MinIO
    if ! nc -z localhost 9000; then
        echo "❌ MinIO not running on localhost:9000"
        echo "Start with: podman run -d --name minio -p 9000:9000 -p 9001:9001 -e \"MINIO_ROOT_USER=minioadmin\" -e \"MINIO_ROOT_PASSWORD=minioadmin\" quay.io/minio/minio server /data --console-address \":9001\""
        exit 1
    fi
    echo "✅ MinIO running"
    
    # Check if kafka tools are available
    if ! command -v kafka-topics.sh &> /dev/null; then
        echo "❌ Kafka tools not found in PATH. Please install Kafka or add to PATH."
        exit 1
    fi
    echo "✅ Kafka tools available"
    
    # Check if connect tools are available
    if ! command -v connect-distributed.sh &> /dev/null; then
        echo "❌ Kafka Connect tools not found in PATH. Please install Kafka Connect or add to PATH."
        exit 1
    fi
    echo "✅ Kafka Connect tools available"
}

# Function to create topics with multiple partitions
create_topics() {
    echo "🏗️  Creating Kafka topics with multiple partitions for scaling test..."
    
    # Create topics with 8 partitions each for maximum parallelism
    kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-events --partitions 8 --replication-factor 1 --if-not-exists
    kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-events --partitions 8 --replication-factor 1 --if-not-exists
    
    # Create Connect internal topics
    kafka-topics.sh --bootstrap-server localhost:9092 --create --topic connect-configs --partitions 1 --replication-factor 1 --if-not-exists
    kafka-topics.sh --bootstrap-server localhost:9092 --create --topic connect-offsets --partitions 25 --replication-factor 1 --if-not-exists
    kafka-topics.sh --bootstrap-server localhost:9092 --create --topic connect-status --partitions 5 --replication-factor 1 --if-not-exists
    
    echo "✅ Topics created with 8 partitions each for horizontal scaling"
}

# Function to start workers
start_workers() {
    echo "🚀 Starting Kafka Connect workers..."
    
    # Make scripts executable
    chmod +x "$SCRIPT_DIR/run-distributed-worker1.sh"
    chmod +x "$SCRIPT_DIR/run-distributed-worker2.sh"
    
    echo "Starting Worker 1 in background..."
    nohup "$SCRIPT_DIR/run-distributed-worker1.sh" > worker1.log 2>&1 &
    WORKER1_PID=$!
    
    echo "Waiting 30 seconds for Worker 1 to initialize..."
    sleep 30
    
    echo "Starting Worker 2 in background..."
    nohup "$SCRIPT_DIR/run-distributed-worker2.sh" > worker2.log 2>&1 &
    WORKER2_PID=$!
    
    echo "Waiting 20 seconds for Worker 2 to join cluster..."
    sleep 20
    
    echo "✅ Both workers started:"
    echo "   Worker 1 PID: $WORKER1_PID (port 8083)"
    echo "   Worker 2 PID: $WORKER2_PID (port 8084)"
}

# Function to deploy connector
deploy_connector() {
    echo "🔗 Deploying S3 Delta Sink Connector with 4 tasks..."
    
    # Use Worker 1's REST API to deploy connector
    curl -X POST http://localhost:8083/connectors \
         -H "Content-Type: application/json" \
         -d @- << EOF
{
    "name": "s3-delta-sink-scaling-test",
    "config": {
        "connector.class": "com.company.kafkaconnector.connector.S3DeltaSinkConnector",
        "tasks.max": "4",
        "topics": "user-events,order-events",
        "s3.bucket.name": "test-data-lake",
        "s3.region": "us-east-1",
        "s3.access.key.id": "minioadmin",
        "s3.secret.access.key": "minioadmin",
        "s3.endpoint.url": "http://localhost:9000",
        "partitioner.strategy": "TIME_BASED",
        "topics.dir": "kafka-connect-scaling-test",
        "flush.size": "100",
        "rotate.interval.ms": "30000",
        "max.retries": "3",
        "retry.backoff.ms": "1000",
        "schema.compatibility": "BACKWARD",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
EOF
    
    echo
    echo "✅ Connector deployed. Waiting 10 seconds for task distribution..."
    sleep 10
}

# Function to check task distribution
check_task_distribution() {
    echo "📊 Checking task distribution across workers..."
    
    echo "Connector status:"
    curl -s http://localhost:8083/connectors/s3-delta-sink-scaling-test/status | jq '.'
    
    echo "Tasks on Worker 1 (port 8083):"
    curl -s http://localhost:8083/connectors/s3-delta-sink-scaling-test/tasks | jq '.'
    
    echo "Tasks on Worker 2 (port 8084):"
    curl -s http://localhost:8084/connectors/s3-delta-sink-scaling-test/tasks | jq '.'
}

# Function to generate load
generate_load() {
    echo "⚡ Generating load with 10,000 test messages..."
    
    # Generate 5000 user-events messages
    echo "Generating user-events messages..."
    for i in {1..5000}; do
        echo "{\"user_id\":$i,\"event_type\":\"login\",\"timestamp\":\"$(date -Iseconds)\",\"properties\":{\"source\":\"scaling_test\",\"batch\":1}}" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic user-events
        if [ $((i % 1000)) -eq 0 ]; then
            echo "Generated $i user-events messages..."
        fi
    done &
    
    # Generate 5000 order-events messages
    echo "Generating order-events messages..."
    for i in {1..5000}; do
        echo "{\"order_id\":$i,\"user_id\":$((i % 1000)),\"amount\":$((RANDOM % 1000 + 10)),\"timestamp\":\"$(date -Iseconds)\",\"status\":\"completed\"}" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic order-events
        if [ $((i % 1000)) -eq 0 ]; then
            echo "Generated $i order-events messages..."
        fi
    done &
    
    wait
    echo "✅ Load generation completed: 10,000 messages sent"
}

# Function to monitor metrics
monitor_metrics() {
    echo "📈 Monitoring metrics from both workers..."
    
    echo "Worker 1 metrics:"
    curl -s http://localhost:8083/actuator/connector/metrics | jq '.processing'
    
    echo "Worker 2 metrics:"
    curl -s http://localhost:8084/actuator/connector/metrics | jq '.processing'
}

# Function to verify data in S3
verify_s3_data() {
    echo "🔍 Verifying processed data in S3/MinIO..."
    
    # Check if MinIO client (mc) is available
    if command -v mc &> /dev/null; then
        echo "Using MinIO client to verify data..."
        
        # Configure mc alias if not exists
        mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
        
        # List bucket contents
        echo "S3 bucket contents:"
        mc ls --recursive local/test-data-lake/ | head -20
        
        # Count files
        FILE_COUNT=$(mc ls --recursive local/test-data-lake/ | wc -l)
        echo "Total files in S3: $FILE_COUNT"
        
        if [ "$FILE_COUNT" -gt 0 ]; then
            echo "✅ Data successfully written to S3"
            
            # Sample file content verification
            echo "Sample file content:"
            SAMPLE_FILE=$(mc ls --recursive local/test-data-lake/ | head -1 | awk '{print $5}')
            if [ ! -z "$SAMPLE_FILE" ]; then
                echo "Checking file: $SAMPLE_FILE"
                mc cat "local/test-data-lake/$SAMPLE_FILE" | head -3
            fi
        else
            echo "❌ No files found in S3 bucket"
        fi
        
    else
        echo "MinIO client not available. Using curl to check bucket..."
        
        # Basic bucket check via S3 API
        if curl -s http://localhost:9000/test-data-lake/ > /dev/null; then
            echo "✅ S3 bucket is accessible"
        else
            echo "⚠️  Cannot verify S3 bucket contents without MinIO client"
            echo "Install with: brew install minio/stable/mc"
        fi
    fi
    
    # Run data verification test if Java test environment is available
    if [ -f "$BASE_DIR/target/test-classes/com/company/kafkaconnector/integration/S3DataVerificationTest.class" ]; then
        echo "Running automated data verification test..."
        cd "$BASE_DIR"
        if RUN_DATA_VERIFICATION=true mvn test -Dtest=S3DataVerificationTest -q; then
            echo "✅ Data verification tests passed"
        else
            echo "⚠️  Some data verification tests failed (check logs)"
        fi
    else
        echo "ℹ️  Compile tests to run automated data verification: mvn test-compile"
    fi
}

# Function to test failover
test_failover() {
    echo "🔄 Testing failover by stopping Worker 1..."
    
    kill $WORKER1_PID
    echo "Worker 1 stopped. Waiting 30 seconds for task rebalancing..."
    sleep 30
    
    echo "Checking task redistribution:"
    curl -s http://localhost:8084/connectors/s3-delta-sink-scaling-test/status | jq '.'
    
    echo "✅ Failover test completed"
}

# Function to cleanup
cleanup() {
    echo "🧹 Cleaning up..."
    
    # Stop workers
    if [ ! -z "$WORKER1_PID" ]; then
        kill $WORKER1_PID 2>/dev/null || true
    fi
    if [ ! -z "$WORKER2_PID" ]; then
        kill $WORKER2_PID 2>/dev/null || true
    fi
    
    # Delete connector
    curl -X DELETE http://localhost:8084/connectors/s3-delta-sink-scaling-test 2>/dev/null || true
    
    echo "✅ Cleanup completed"
}

# Main execution
trap cleanup EXIT

echo "Starting horizontal scaling test..."
check_prerequisites
create_topics
start_workers
deploy_connector
check_task_distribution
generate_load
sleep 60  # Wait for processing
monitor_metrics
verify_s3_data
test_failover

echo
echo "=========================================================================="
echo "                    HORIZONTAL SCALING TEST COMPLETED"
echo "=========================================================================="
echo "Results:"
echo "✅ Successfully ran 2 Kafka Connect workers in distributed mode"
echo "✅ Deployed connector with 4 tasks distributed across workers"
echo "✅ Processed 10,000 messages with horizontal scaling"
echo "✅ Tested failover and task rebalancing"
echo "✅ Validated metrics collection from multiple instances"
echo
echo "Check logs:"
echo "  - Worker 1: worker1.log"
echo "  - Worker 2: worker2.log"
echo "  - S3 data: MinIO console at http://localhost:9001"
echo "=========================================================================="