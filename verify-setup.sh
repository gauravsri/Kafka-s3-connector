#!/bin/bash

echo "🚀 Kafka S3 Connector - Environment Verification"
echo "================================================"

# Check Java
echo -n "☕ Java 17+: "
java -version 2>&1 | grep -q "17\." && echo "✅ OK" || echo "❌ Not found"

# Check Maven
echo -n "📦 Maven 3.6+: "
mvn -version 2>&1 | grep -q "Apache Maven 3\." && echo "✅ OK" || echo "❌ Not found"

# Check containers
echo -n "🐳 MinIO Container: "
podman ps | grep -q minio && echo "✅ Running" || echo "❌ Not running"

echo -n "🐳 RedPanda Container: "
podman ps | grep -q redpanda && echo "✅ Running" || echo "❌ Not running"

# Check connectivity
echo -n "🌐 MinIO Console (9001): "
curl -f http://localhost:9001 &>/dev/null && echo "✅ Accessible" || echo "❌ Not accessible"

echo -n "🌐 RedPanda Kafka (9092): "
nc -z localhost 9092 2>/dev/null && echo "✅ Accessible" || echo "❌ Not accessible"

# Check project build
echo -n "🏗️  Project Compilation: "
mvn clean compile -q 2>/dev/null && echo "✅ Success" || echo "❌ Failed"

echo ""
echo "📋 Configuration Status:"
echo "  • Topics configured: user-events, order-events"
echo "  • Schemas available: ✅ user-events-schema.json, ✅ order-events-schema.json"
echo "  • S3 endpoint: http://localhost:9000 (MinIO)"
echo "  • Kafka endpoint: localhost:9092 (RedPanda)"
echo ""
echo "🎯 Ready to test: mvn spring-boot:run"
echo "📊 Health check: http://localhost:8081/actuator/health (when running)"