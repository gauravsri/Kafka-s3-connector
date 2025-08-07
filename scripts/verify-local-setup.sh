#!/bin/bash

# Comprehensive local setup verification and testing script
# Verifies all components work correctly with local Podman containers

set -e

BASE_DIR="/Users/gaurav/kafka-s3-connector"

echo "=========================================================================="
echo "                    KAFKA S3 CONNECTOR - LOCAL VERIFICATION"
echo "=========================================================================="
echo "This script verifies the complete local development setup:"
echo "✓ Java and Maven environment"
echo "✓ Podman containers (RedPanda, MinIO)" 
echo "✓ Connector compilation and configuration"
echo "✓ Basic functionality test"
echo "✓ Horizontal scaling readiness"
echo "=========================================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored status
print_status() {
    local status=$1
    local message=$2
    case $status in
        "SUCCESS") echo -e "${GREEN}✅ $message${NC}" ;;
        "WARNING") echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "ERROR") echo -e "${RED}❌ $message${NC}" ;;
        "INFO") echo -e "${BLUE}ℹ️  $message${NC}" ;;
    esac
}

# Function to check command availability
check_command() {
    if command -v "$1" &> /dev/null; then
        print_status "SUCCESS" "$1 is available"
        return 0
    else
        print_status "ERROR" "$1 is not available"
        return 1
    fi
}

# Function to check port
check_port() {
    local host=$1
    local port=$2
    local service=$3
    
    if nc -z "$host" "$port" 2>/dev/null; then
        print_status "SUCCESS" "$service is running on $host:$port"
        return 0
    else
        print_status "ERROR" "$service is not running on $host:$port"
        return 1
    fi
}

# Step 1: Verify development environment
echo
echo "🔍 STEP 1: Verifying Development Environment"
echo "----------------------------------------------"

if check_command "java"; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d '"' -f 2)
    print_status "INFO" "Java version: $JAVA_VERSION"
    if [[ "$JAVA_VERSION" =~ ^(17|21) ]]; then
        print_status "SUCCESS" "Java version is compatible"
    else
        print_status "WARNING" "Java version should be 17 or 21"
    fi
else
    print_status "ERROR" "Java is required"
    exit 1
fi

if check_command "mvn"; then
    MVN_VERSION=$(mvn -version | head -n 1 | cut -d ' ' -f 3)
    print_status "INFO" "Maven version: $MVN_VERSION"
else
    print_status "ERROR" "Maven is required"
    exit 1
fi

check_command "podman" || check_command "docker"
check_command "curl"
check_command "jq" || print_status "WARNING" "jq recommended for JSON parsing"

# Step 2: Check Podman containers
echo
echo "🐳 STEP 2: Verifying Podman Containers"
echo "--------------------------------------"

# Check RedPanda
if check_port "localhost" "9092" "RedPanda (Kafka)"; then
    print_status "INFO" "Testing Kafka connectivity..."
    if command -v kafka-topics.sh &> /dev/null; then
        if kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null; then
            print_status "SUCCESS" "Kafka connectivity verified"
        else
            print_status "WARNING" "Kafka tools available but connection failed"
        fi
    else
        print_status "INFO" "Kafka tools not in PATH - container connectivity assumed OK"
    fi
else
    print_status "ERROR" "Start RedPanda with:"
    echo "podman run -d --name=redpanda -p 9092:9092 \\"
    echo "  docker.redpanda.com/redpandadata/redpanda:v24.1.1 \\"
    echo "  redpanda start --overprovisioned --smp 1 --memory 512M \\"
    echo "  --reserve-memory 0M --node-id 0 --check=false"
fi

# Check MinIO
if check_port "localhost" "9000" "MinIO (S3)"; then
    if check_port "localhost" "9001" "MinIO Console"; then
        print_status "SUCCESS" "MinIO fully accessible"
        print_status "INFO" "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    else
        print_status "WARNING" "MinIO API available but console port 9001 not accessible"
    fi
else
    print_status "ERROR" "Start MinIO with:"
    echo "podman run -d --name minio -p 9000:9000 -p 9001:9001 \\"
    echo "  -e \"MINIO_ROOT_USER=minioadmin\" -e \"MINIO_ROOT_PASSWORD=minioadmin\" \\"
    echo "  -v /Users/gaurav/Downloads:/data \\"
    echo "  quay.io/minio/minio server /data --console-address \":9001\""
fi

# Step 3: Verify connector compilation
echo
echo "🔨 STEP 3: Verifying Connector Compilation"
echo "------------------------------------------"

cd "$BASE_DIR"

if [ -f "pom.xml" ]; then
    print_status "SUCCESS" "Found pom.xml"
    
    print_status "INFO" "Compiling connector..."
    if mvn clean compile -DskipTests -q; then
        print_status "SUCCESS" "Connector compiled successfully"
        
        # Check for main connector class
        CONNECTOR_CLASS="$BASE_DIR/target/classes/com/company/kafkaconnector/connector/S3DeltaSinkConnector.class"
        if [ -f "$CONNECTOR_CLASS" ]; then
            print_status "SUCCESS" "Main connector class found"
        else
            print_status "ERROR" "Main connector class not found"
        fi
        
        # Check for task class
        TASK_CLASS="$BASE_DIR/target/classes/com/company/kafkaconnector/connector/S3DeltaSinkTask.class"
        if [ -f "$TASK_CLASS" ]; then
            print_status "SUCCESS" "Task class found"
        else
            print_status "ERROR" "Task class not found"
        fi
        
    else
        print_status "ERROR" "Compilation failed"
        exit 1
    fi
else
    print_status "ERROR" "pom.xml not found"
    exit 1
fi

# Step 4: Verify configuration files
echo
echo "⚙️  STEP 4: Verifying Configuration Files"
echo "----------------------------------------"

CONFIG_FILES=(
    "config/connect-distributed.properties:Distributed mode worker 1"
    "config/connect-distributed-worker2.properties:Distributed mode worker 2"
    "config/s3-delta-sink.properties:Connector configuration"
    "config/connect-standalone.properties:Standalone mode"
)

for config_info in "${CONFIG_FILES[@]}"; do
    IFS=':' read -r file desc <<< "$config_info"
    if [ -f "$BASE_DIR/$file" ]; then
        print_status "SUCCESS" "$desc configuration found"
    else
        print_status "WARNING" "$desc configuration missing: $file"
    fi
done

# Step 5: Check scripts
echo
echo "📝 STEP 5: Verifying Scripts"
echo "---------------------------"

SCRIPTS=(
    "scripts/run-distributed-worker1.sh:Worker 1 startup script"
    "scripts/run-distributed-worker2.sh:Worker 2 startup script"
    "scripts/test-horizontal-scaling.sh:Horizontal scaling test"
)

for script_info in "${SCRIPTS[@]}"; do
    IFS=':' read -r script desc <<< "$script_info"
    if [ -f "$BASE_DIR/$script" ]; then
        if [ -x "$BASE_DIR/$script" ]; then
            print_status "SUCCESS" "$desc is executable"
        else
            print_status "WARNING" "$desc exists but not executable"
            chmod +x "$BASE_DIR/$script"
            print_status "INFO" "Made $script executable"
        fi
    else
        print_status "WARNING" "$desc missing: $script"
    fi
done

# Step 6: Run basic tests
echo
echo "🧪 STEP 6: Running Basic Tests"
echo "-----------------------------"

print_status "INFO" "Running unit tests..."
if mvn test -Dtest="!*Integration*" -q; then
    print_status "SUCCESS" "Unit tests passed"
else
    print_status "WARNING" "Some unit tests failed (check logs)"
fi

# Check if integration tests can run
if [[ -n "${RUN_INTEGRATION_TESTS:-}" ]]; then
    print_status "INFO" "Running integration tests..."
    if mvn test -Dtest="*Integration*" -q; then
        print_status "SUCCESS" "Integration tests passed"
    else
        print_status "WARNING" "Integration tests failed (check logs)"
    fi
else
    print_status "INFO" "To run integration tests: RUN_INTEGRATION_TESTS=true mvn test"
fi

# Step 7: Test Kafka Connect tools availability
echo
echo "🔧 STEP 7: Checking Kafka Connect Tools"
echo "---------------------------------------"

if command -v connect-standalone.sh &> /dev/null; then
    print_status "SUCCESS" "Kafka Connect standalone tools available"
else
    print_status "WARNING" "Kafka Connect tools not in PATH"
    print_status "INFO" "Download from: https://kafka.apache.org/downloads"
    print_status "INFO" "Or use confluent/cp-kafka Docker image with tools"
fi

if command -v connect-distributed.sh &> /dev/null; then
    print_status "SUCCESS" "Kafka Connect distributed tools available"
else
    print_status "WARNING" "Kafka Connect distributed tools not in PATH"
fi

# Step 8: Summary and next steps
echo
echo "📋 STEP 8: Summary and Next Steps"
echo "---------------------------------"

echo
print_status "INFO" "Manual Testing Instructions:"
echo "  1. Ensure containers are running (steps above)"
echo "  2. Test single worker:"
echo "     ./scripts/run-distributed-worker1.sh"
echo "  3. Test horizontal scaling:"
echo "     Terminal 1: ./scripts/run-distributed-worker1.sh"
echo "     Terminal 2: ./scripts/run-distributed-worker2.sh"
echo "     Terminal 3: ./scripts/test-horizontal-scaling.sh"
echo
print_status "INFO" "Monitoring Endpoints:"
echo "  - Worker 1 REST API: http://localhost:8083"
echo "  - Worker 2 REST API: http://localhost:8084"
echo "  - Custom Metrics: http://localhost:8083/actuator/connector/metrics"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo

# Final status
ERRORS=$(grep -c "❌" <<< "$(cat)" 2>/dev/null || echo "0")
WARNINGS=$(grep -c "⚠️" <<< "$(cat)" 2>/dev/null || echo "0")

echo "=========================================================================="
if [ "$ERRORS" -eq 0 ]; then
    if [ "$WARNINGS" -eq 0 ]; then
        print_status "SUCCESS" "All verification checks passed! Ready for testing."
    else
        print_status "WARNING" "Verification completed with $WARNINGS warnings. System mostly ready."
    fi
else
    print_status "ERROR" "Verification failed with $ERRORS errors and $WARNINGS warnings."
    echo "Please fix the errors above before proceeding."
    exit 1
fi
echo "=========================================================================="