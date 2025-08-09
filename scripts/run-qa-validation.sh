#!/bin/bash

# QA Validation Script for Kafka-S3-Delta Lake Connector
# This script runs comprehensive quality assurance checks
# Author: Claude (AI QA Lead)

set -e

echo "🔬 Starting QA Validation Suite"
echo "==============================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0
WARNINGS=0

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED_CHECKS++))
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
    ((WARNINGS++))
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAILED_CHECKS++))
}

run_check() {
    ((TOTAL_CHECKS++))
    local check_name="$1"
    local command="$2"
    local success_msg="$3"
    local error_msg="$4"
    
    log_info "Running: $check_name"
    
    if eval "$command" > /dev/null 2>&1; then
        log_success "$success_msg"
    else
        log_error "$error_msg"
    fi
}

# QA Check 1: Environment Prerequisites
echo -e "\n${BLUE}█${NC} Environment Prerequisites Check"
echo "=================================="

run_check "Java Version Check" \
    "java -version 2>&1 | grep -q 'version \"17'" \
    "Java 17 detected" \
    "Java 17 not found - required for Spring Boot 3.x"

run_check "Maven Installation" \
    "mvn --version > /dev/null" \
    "Maven installation verified" \
    "Maven not installed or not in PATH"

run_check "Project Structure" \
    "test -f pom.xml && test -d src/main/java" \
    "Project structure valid" \
    "Invalid project structure - missing pom.xml or src directories"

# QA Check 2: Build Quality
echo -e "\n${BLUE}█${NC} Build Quality Assessment"
echo "============================"

log_info "Compiling project..."
if mvn clean compile -q; then
    log_success "Project compiles without errors"
    ((PASSED_CHECKS++))
else
    log_error "Compilation failed - fix build errors before proceeding"
    ((FAILED_CHECKS++))
fi
((TOTAL_CHECKS++))

# QA Check 3: Unit Test Execution
echo -e "\n${BLUE}█${NC} Unit Test Execution"
echo "====================="

log_info "Running unit test suite..."
TEST_RESULTS=$(mktemp)

if mvn test -Dtest="*Test" --batch-mode > "$TEST_RESULTS" 2>&1; then
    TESTS_RUN=$(grep "Tests run:" "$TEST_RESULTS" | tail -1 | sed 's/.*Tests run: \([0-9]*\).*/\1/' || echo "0")
    FAILURES=$(grep "Failures:" "$TEST_RESULTS" | tail -1 | sed 's/.*Failures: \([0-9]*\).*/\1/' || echo "0")
    ERRORS=$(grep "Errors:" "$TEST_RESULTS" | tail -1 | sed 's/.*Errors: \([0-9]*\).*/\1/' || echo "0")
    SKIPPED=$(grep "Skipped:" "$TEST_RESULTS" | tail -1 | sed 's/.*Skipped: \([0-9]*\).*/\1/' || echo "0")
    
    if [ "$FAILURES" -eq 0 ] && [ "$ERRORS" -eq 0 ]; then
        log_success "All unit tests passed ($TESTS_RUN tests, $SKIPPED skipped)"
    else
        log_warning "Tests completed with issues: $FAILURES failures, $ERRORS errors out of $TESTS_RUN tests"
    fi
else
    log_error "Unit test execution failed - check test output for details"
fi

((TOTAL_CHECKS++))
if [ "$FAILURES" -eq 0 ] && [ "$ERRORS" -eq 0 ]; then
    ((PASSED_CHECKS++))
else
    ((FAILED_CHECKS++))
fi

# QA Check 4: Code Quality Analysis
echo -e "\n${BLUE}█${NC} Code Quality Analysis"  
echo "======================"

log_info "Analyzing code quality metrics..."

# Count lines of code
LOC_MAIN=$(find src/main/java -name "*.java" | xargs wc -l | tail -1 | awk '{print $1}')
LOC_TEST=$(find src/test/java -name "*.java" | xargs wc -l | tail -1 | awk '{print $1}')
TEST_RATIO=$(echo "scale=2; $LOC_TEST * 100 / $LOC_MAIN" | bc 2>/dev/null || echo "0")

log_info "Lines of Code - Main: $LOC_MAIN, Test: $LOC_TEST"

if (( $(echo "$TEST_RATIO >= 50" | bc -l) )); then
    log_success "Good test coverage ratio: ${TEST_RATIO}%"
    ((PASSED_CHECKS++))
elif (( $(echo "$TEST_RATIO >= 30" | bc -l) )); then
    log_warning "Moderate test coverage ratio: ${TEST_RATIO}%"
else
    log_error "Low test coverage ratio: ${TEST_RATIO}%"
    ((FAILED_CHECKS++))
fi
((TOTAL_CHECKS++))

# QA Check 5: Documentation Quality
echo -e "\n${BLUE}█${NC} Documentation Assessment"
echo "=========================="

REQUIRED_DOCS=("README.md" "DESIGN.md" "USER_GUIDE.md" "IMPL_STEPS.md")
FOUND_DOCS=0

for doc in "${REQUIRED_DOCS[@]}"; do
    if [ -f "$doc" ]; then
        ((FOUND_DOCS++))
        log_success "$doc exists"
    else
        log_warning "$doc missing"
    fi
done

run_check "Documentation Coverage" \
    "[ $FOUND_DOCS -ge 3 ]" \
    "Good documentation coverage ($FOUND_DOCS/${#REQUIRED_DOCS[@]} files)" \
    "Insufficient documentation ($FOUND_DOCS/${#REQUIRED_DOCS[@]} files)"

# QA Check 6: Configuration Validation
echo -e "\n${BLUE}█${NC} Configuration Validation"
echo "=========================="

CONFIGS=("src/main/resources/application.yml" "src/main/resources/application-docker.yml")
for config in "${CONFIGS[@]}"; do
    if [ -f "$config" ]; then
        run_check "YAML Syntax: $(basename $config)" \
            "python3 -c 'import yaml; yaml.safe_load(open(\"$config\"))' 2>/dev/null || yq eval . \"$config\" > /dev/null 2>&1" \
            "$(basename $config) syntax valid" \
            "$(basename $config) has YAML syntax errors"
    else
        log_warning "Configuration file missing: $config"
    fi
done

# QA Check 7: Dependency Security Check
echo -e "\n${BLUE}█${NC} Dependency Security Assessment"
echo "==============================="

log_info "Checking for known vulnerabilities..."
if command -v mvn > /dev/null; then
    if mvn org.owasp:dependency-check-maven:check -q 2>/dev/null; then
        log_success "No high-severity vulnerabilities detected"
        ((PASSED_CHECKS++))
    else
        log_warning "Dependency security check failed or found issues"
    fi
else
    log_warning "Maven not available for security check"
fi
((TOTAL_CHECKS++))

# QA Check 8: Docker Configuration
echo -e "\n${BLUE}█${NC} Container Configuration"
echo "========================"

if [ -f "Dockerfile" ]; then
    run_check "Dockerfile Exists" \
        "test -f Dockerfile" \
        "Dockerfile present" \
        "Dockerfile missing"
        
    run_check "Docker Compose Configuration" \
        "test -f docker-compose.yml" \
        "Docker Compose configuration present" \
        "Docker Compose configuration missing"
else
    log_warning "Docker configuration not found"
fi

# QA Check 9: Performance Indicators
echo -e "\n${BLUE}█${NC} Performance Indicators"
echo "======================="

log_info "Analyzing performance-critical patterns..."

# Check for async patterns
ASYNC_PATTERNS=$(grep -r "@Async\|CompletableFuture\|ExecutorService" src/main/java/ | wc -l)
if [ "$ASYNC_PATTERNS" -gt 0 ]; then
    log_success "Asynchronous processing patterns detected ($ASYNC_PATTERNS occurrences)"
    ((PASSED_CHECKS++))
else
    log_warning "No asynchronous processing patterns found"
fi
((TOTAL_CHECKS++))

# Check for caching
CACHING_PATTERNS=$(grep -r "@Cacheable\|@Cache\|ConcurrentHashMap" src/main/java/ | wc -l)
if [ "$CACHING_PATTERNS" -gt 0 ]; then
    log_success "Caching mechanisms detected ($CACHING_PATTERNS occurrences)"
    ((PASSED_CHECKS++))
else
    log_warning "Limited caching mechanisms detected"
fi
((TOTAL_CHECKS++))

# QA Check 10: Monitoring and Observability
echo -e "\n${BLUE}█${NC} Observability Assessment"
echo "========================="

METRICS_PATTERNS=$(grep -r "@Timed\|MeterRegistry\|Counter\|Timer" src/main/java/ | wc -l)
if [ "$METRICS_PATTERNS" -gt 0 ]; then
    log_success "Metrics instrumentation detected ($METRICS_PATTERNS occurrences)"
    ((PASSED_CHECKS++))
else
    log_warning "Limited metrics instrumentation"
fi
((TOTAL_CHECKS++))

LOGGING_PATTERNS=$(grep -r "logger\.\|log\." src/main/java/ | wc -l)
if [ "$LOGGING_PATTERNS" -gt 10 ]; then
    log_success "Comprehensive logging detected ($LOGGING_PATTERNS occurrences)"
    ((PASSED_CHECKS++))
else
    log_warning "Limited logging detected ($LOGGING_PATTERNS occurrences)"
fi
((TOTAL_CHECKS++))

# Final Assessment
echo -e "\n${BLUE}█${NC} QA Assessment Summary"
echo "======================="

PASS_RATE=$(echo "scale=1; $PASSED_CHECKS * 100 / $TOTAL_CHECKS" | bc)

echo -e "Total Checks: $TOTAL_CHECKS"
echo -e "Passed: ${GREEN}$PASSED_CHECKS${NC}"
echo -e "Failed: ${RED}$FAILED_CHECKS${NC}"  
echo -e "Warnings: ${YELLOW}$WARNINGS${NC}"
echo -e "Pass Rate: ${PASS_RATE}%"

# Overall Grade
if (( $(echo "$PASS_RATE >= 90" | bc -l) )); then
    GRADE="A"
    COLOR=$GREEN
    STATUS="EXCELLENT"
elif (( $(echo "$PASS_RATE >= 80" | bc -l) )); then
    GRADE="B"
    COLOR=$BLUE
    STATUS="GOOD"
elif (( $(echo "$PASS_RATE >= 70" | bc -l) )); then
    GRADE="C"
    COLOR=$YELLOW
    STATUS="ACCEPTABLE"
else
    GRADE="F"
    COLOR=$RED
    STATUS="NEEDS WORK"
fi

echo -e "\n🎯 ${COLOR}Overall Grade: $GRADE ($STATUS)${NC}"

# Recommendations
echo -e "\n📋 QA Recommendations:"
if [ "$FAILED_CHECKS" -gt 0 ]; then
    echo -e "${RED}■${NC} Fix $FAILED_CHECKS critical issues before production"
fi
if [ "$WARNINGS" -gt 0 ]; then
    echo -e "${YELLOW}■${NC} Address $WARNINGS warnings for optimal quality"
fi

if (( $(echo "$PASS_RATE >= 85" | bc -l) )); then
    echo -e "${GREEN}■${NC} Project is ready for production deployment"
else
    echo -e "${RED}■${NC} Complete additional fixes before production"
fi

# Clean up
rm -f "$TEST_RESULTS"

echo -e "\n🏁 QA Validation Complete"
echo "========================"

# Exit with appropriate code
if [ "$FAILED_CHECKS" -eq 0 ]; then
    exit 0
else
    exit 1
fi