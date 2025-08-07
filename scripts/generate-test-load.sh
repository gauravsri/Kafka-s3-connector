#!/bin/bash

# Load generation script for testing horizontal scaling
# Generates realistic test data for user-events and order-events topics

set -e

KAFKA_HOST="localhost:9092"
TOTAL_MESSAGES=${1:-10000}
BATCH_SIZE=100
TOPICS=("user-events" "order-events")

echo "=========================================================================="
echo "                          LOAD GENERATION TEST"
echo "=========================================================================="
echo "Generating $TOTAL_MESSAGES messages across ${#TOPICS[@]} topics"
echo "Kafka broker: $KAFKA_HOST"
echo "Batch size: $BATCH_SIZE messages"
echo "=========================================================================="

# Check if Kafka tools are available
if ! command -v kafka-console-producer.sh &> /dev/null; then
    echo "❌ kafka-console-producer.sh not found in PATH"
    echo "Please install Kafka tools or add to PATH"
    exit 1
fi

# Check Kafka connectivity
if ! nc -z localhost 9092; then
    echo "❌ Cannot connect to Kafka at $KAFKA_HOST"
    echo "Please start RedPanda container"
    exit 1
fi

# Function to generate user events
generate_user_events() {
    local count=$1
    local topic="user-events"
    
    echo "📊 Generating $count user-events messages..."
    
    for ((i=1; i<=count; i++)); do
        local user_id=$((RANDOM % 1000 + 1))
        local event_types=("login" "logout" "page_view" "click" "purchase" "signup")
        local event_type=${event_types[$((RANDOM % ${#event_types[@]}))]}
        local timestamp=$(date -Iseconds)
        local session_id="session_$((RANDOM % 100))"
        
        local message=$(cat <<EOF
{
  "user_id": $user_id,
  "event_type": "$event_type",
  "timestamp": "$timestamp",
  "session_id": "$session_id",
  "properties": {
    "source": "load_test",
    "batch_id": "$((i / BATCH_SIZE))",
    "sequence": $i,
    "user_agent": "LoadTestAgent/1.0",
    "ip_address": "192.168.1.$((RANDOM % 254 + 1))",
    "platform": "${event_types[$((RANDOM % 3))]}"
  },
  "metadata": {
    "generated_at": "$timestamp",
    "test_run": "horizontal_scaling",
    "partition_key": "$((user_id % 8))"
  }
}
EOF
)
        
        # Send message with partition key
        echo "$message" | kafka-console-producer.sh \
            --bootstrap-server $KAFKA_HOST \
            --topic $topic \
            --property "key.separator=:" \
            --property "parse.key=false" \
            > /dev/null
        
        # Progress indicator
        if [ $((i % (count / 10))) -eq 0 ] && [ $i -ne $count ]; then
            local percent=$((i * 100 / count))
            echo "  Progress: $percent% ($i/$count messages)"
        fi
        
        # Small delay for realistic load
        if [ $((i % BATCH_SIZE)) -eq 0 ]; then
            sleep 0.1
        fi
    done
    
    echo "✅ Completed generating $count user-events messages"
}

# Function to generate order events
generate_order_events() {
    local count=$1
    local topic="order-events"
    
    echo "📊 Generating $count order-events messages..."
    
    for ((i=1; i<=count; i++)); do
        local order_id="order_$(date +%s)_$i"
        local user_id=$((RANDOM % 1000 + 1))
        local amount=$((RANDOM % 50000 + 1000)) # $10.00 to $500.00
        local amount_dollars=$(echo "scale=2; $amount/100" | bc)
        local statuses=("pending" "processing" "completed" "cancelled" "refunded")
        local status=${statuses[$((RANDOM % ${#statuses[@]}))]}
        local timestamp=$(date -Iseconds)
        local products=("laptop" "phone" "tablet" "headphones" "keyboard" "mouse")
        local product=${products[$((RANDOM % ${#products[@]}))]}
        
        local message=$(cat <<EOF
{
  "order_id": "$order_id",
  "user_id": $user_id,
  "amount": $amount_dollars,
  "currency": "USD",
  "status": "$status",
  "timestamp": "$timestamp",
  "items": [
    {
      "product": "$product",
      "quantity": $((RANDOM % 3 + 1)),
      "price": $amount_dollars
    }
  ],
  "shipping": {
    "method": "standard",
    "address": {
      "city": "TestCity",
      "state": "TS",
      "country": "US",
      "zip": "${RANDOM:0:5}"
    }
  },
  "metadata": {
    "generated_at": "$timestamp",
    "test_run": "horizontal_scaling", 
    "batch_id": "$((i / BATCH_SIZE))",
    "sequence": $i,
    "partition_key": "$((user_id % 8))"
  }
}
EOF
)
        
        # Send message
        echo "$message" | kafka-console-producer.sh \
            --bootstrap-server $KAFKA_HOST \
            --topic $topic \
            --property "key.separator=:" \
            --property "parse.key=false" \
            > /dev/null
        
        # Progress indicator
        if [ $((i % (count / 10))) -eq 0 ] && [ $i -ne $count ]; then
            local percent=$((i * 100 / count))
            echo "  Progress: $percent% ($i/$count messages)"
        fi
        
        # Small delay for realistic load
        if [ $((i % BATCH_SIZE)) -eq 0 ]; then
            sleep 0.1
        fi
    done
    
    echo "✅ Completed generating $count order-events messages"
}

# Function to create topics if they don't exist
create_topics() {
    echo "🏗️  Creating topics with multiple partitions..."
    
    for topic in "${TOPICS[@]}"; do
        kafka-topics.sh --bootstrap-server $KAFKA_HOST \
            --create --topic $topic --partitions 8 --replication-factor 1 \
            --if-not-exists > /dev/null 2>&1
        echo "  ✅ Topic '$topic' ready (8 partitions)"
    done
}

# Function to show topic info
show_topic_info() {
    echo "📋 Topic Information:"
    for topic in "${TOPICS[@]}"; do
        echo "  Topic: $topic"
        kafka-topics.sh --bootstrap-server $KAFKA_HOST \
            --describe --topic $topic | grep -E "(Topic:|PartitionCount:|ReplicationFactor:)" | head -1
    done
}

# Main execution
main() {
    local start_time=$(date +%s)
    
    # Create topics
    create_topics
    show_topic_info
    
    echo
    echo "🚀 Starting load generation..."
    echo "Start time: $(date)"
    
    # Split messages evenly between topics
    local messages_per_topic=$((TOTAL_MESSAGES / ${#TOPICS[@]}))
    
    # Generate load in parallel for better performance
    generate_user_events $messages_per_topic &
    USER_PID=$!
    
    generate_order_events $messages_per_topic &
    ORDER_PID=$!
    
    # Wait for both generators to complete
    wait $USER_PID
    wait $ORDER_PID
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local messages_per_second=$((TOTAL_MESSAGES / duration))
    
    echo
    echo "=========================================================================="
    echo "                          LOAD GENERATION COMPLETED"
    echo "=========================================================================="
    echo "📊 Results:"
    echo "  Total messages: $TOTAL_MESSAGES"
    echo "  Duration: ${duration}s"
    echo "  Rate: ${messages_per_second} messages/second"
    echo "  Topics: ${TOPICS[*]}"
    echo "  Partitions per topic: 8"
    echo
    echo "🔍 Verification:"
    echo "  Check message counts:"
    for topic in "${TOPICS[@]}"; do
        echo "    kafka-console-consumer.sh --bootstrap-server $KAFKA_HOST --topic $topic --from-beginning --timeout-ms 5000 | wc -l"
    done
    echo
    echo "  Monitor connector processing at:"
    echo "    http://localhost:8083/actuator/connector/metrics"
    echo "    http://localhost:8084/actuator/connector/metrics"
    echo "=========================================================================="
}

# Cleanup function
cleanup() {
    echo "🧹 Cleaning up background processes..."
    jobs -p | xargs -r kill 2>/dev/null || true
}

trap cleanup EXIT

# Run main function
main "$@"