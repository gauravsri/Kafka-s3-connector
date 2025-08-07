#!/bin/bash

echo "Sending test messages to trigger batch flush..."

# Send messages to orders topic (batch size 500)
for i in {1..50}; do
  MSG="{\"order_id\":\"order_$(printf '%03d' $i)\",\"customer_id\":\"cust_$(printf '%03d' $i)\",\"order_status\":\"created\",\"timestamp\":\"2025-08-07T18:42:00Z\",\"total_amount\":99.99,\"currency\":\"USD\"}"
  echo "$MSG" | podman exec -i redpanda rpk topic produce orders.lifecycle.v2 --quiet
  if [ $((i % 10)) -eq 0 ]; then
    echo "Sent $i order messages..."
  fi
done

echo "Sent 50 order messages. Checking if batch will be flushed..."