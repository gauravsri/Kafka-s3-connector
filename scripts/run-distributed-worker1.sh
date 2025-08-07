#!/bin/bash

# Script to run Kafka Connect Worker 1 in distributed mode
# This worker will join the connect-cluster-s3-delta group and coordinate with other workers

set -e

echo "==================== Kafka Connect Distributed Worker 1 ===================="
echo "Starting Kafka Connect Worker 1 on port 8083"
echo "Cluster ID: connect-cluster-s3-delta"
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

# Check if Kafka/RedPanda is running
echo "Checking Kafka/RedPanda connection..."
if ! nc -z localhost 9092; then
    echo "ERROR: Kafka/RedPanda not available on localhost:9092"
    echo "Start RedPanda with: podman run -d --name=redpanda -p 9092:9092 docker.redpanda.com/redpandadata/redpanda:v24.1.1 redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M --node-id 0 --check=false"
    exit 1
fi

# Check if MinIO is running
echo "Checking MinIO connection..."
if ! nc -z localhost 9000; then
    echo "ERROR: MinIO not available on localhost:9000"
    echo "Start MinIO with: podman run -d --name minio -p 9000:9000 -p 9001:9001 -e \"MINIO_ROOT_USER=minioadmin\" -e \"MINIO_ROOT_PASSWORD=minioadmin\" -v /Users/gaurav/Downloads:/data quay.io/minio/minio server /data --console-address \":9001\""
    exit 1
fi

echo "All prerequisites met. Starting Kafka Connect Worker 1..."
echo

# Set Java options for better performance
export JAVA_OPTS="-Xmx1G -Xms512M -XX:+UseG1GC -Dfile.encoding=UTF-8"

# Start Kafka Connect in distributed mode
echo "Command: connect-distributed.sh /Users/gaurav/kafka-s3-connector/config/connect-distributed.properties"
echo

# Note: This assumes Kafka Connect is available in PATH
# If not, adjust the path to your Kafka installation
connect-distributed.sh /Users/gaurav/kafka-s3-connector/config/connect-distributed.properties

echo
echo "Kafka Connect Worker 1 stopped."