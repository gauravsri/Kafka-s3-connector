#!/bin/bash

# Script to run Kafka Connect Worker 2 in distributed mode
# This worker will join the same connect-cluster-s3-delta group as Worker 1

set -e

echo "==================== Kafka Connect Distributed Worker 2 ===================="
echo "Starting Kafka Connect Worker 2 on port 8084"
echo "Cluster ID: connect-cluster-s3-delta (same as Worker 1)"
echo "Plugin Path: /Users/gaurav/kafka-s3-connector/target"
echo

# Ensure the plugin directory exists and contains our connector
if [ ! -d "/Users/gaurav/kafka-s3-connector/target" ]; then
    echo "ERROR: Plugin directory not found. Run 'mvn compile' first."
    exit 1
fi

if [ ! -f "/Users/gaurav/kafka-s3-connector/target/classes/com/company/kafkaconnector/connector/S3DeltaSinkConnector.class" ]; then
    echo "ERROR: Connector class not found. Run 'mvn compile' first."
    exit 1
fi

# Check if Worker 1 is already running
echo "Checking if Worker 1 is running on port 8083..."
if nc -z localhost 8083; then
    echo "✓ Worker 1 detected on port 8083"
else
    echo "WARNING: Worker 1 not detected on port 8083. Start Worker 1 first for optimal distributed testing."
    echo "Run: ./scripts/run-distributed-worker1.sh"
    echo "Continuing anyway - Worker 2 can start independently..."
fi

# Check if Kafka/RedPanda is running
echo "Checking Kafka/RedPanda connection..."
if ! nc -z localhost 9092; then
    echo "ERROR: Kafka/RedPanda not available on localhost:9092"
    exit 1
fi

# Check if MinIO is running
echo "Checking MinIO connection..."
if ! nc -z localhost 9000; then
    echo "ERROR: MinIO not available on localhost:9000"
    exit 1
fi

echo "All prerequisites met. Starting Kafka Connect Worker 2..."
echo

# Set Java options for better performance
export JAVA_OPTS="-Xmx1G -Xms512M -XX:+UseG1GC -Dfile.encoding=UTF-8"

# Start Kafka Connect in distributed mode with Worker 2 configuration
echo "Command: connect-distributed.sh /Users/gaurav/kafka-s3-connector/config/connect-distributed-worker2.properties"
echo

# Note: This assumes Kafka Connect is available in PATH
# If not, adjust the path to your Kafka installation
connect-distributed.sh /Users/gaurav/kafka-s3-connector/config/connect-distributed-worker2.properties

echo
echo "Kafka Connect Worker 2 stopped."