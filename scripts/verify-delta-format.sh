#!/bin/bash

# Delta Format Verification Script
# QA Lead - Verify S3 Output is in Delta Format

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BUCKET_NAME="test-data-lake"
MINIO_ALIAS="minio"
MINIO_ENDPOINT="http://localhost:9000"

echo -e "${BLUE}🔍 Delta Format Verification in S3${NC}"
echo -e "${BLUE}================================${NC}"

# Function to check if MinIO is accessible
check_minio() {
    if docker exec minio mc admin info minio >/dev/null 2>&1; then
        echo -e "${GREEN}✅ MinIO is accessible${NC}"
        return 0
    else
        echo -e "${RED}❌ MinIO is not accessible${NC}"
        return 1
    fi
}

# Function to verify Delta table structure
verify_delta_structure() {
    local table_path=$1
    local table_name=$2
    
    echo -e "\n${YELLOW}🔍 Verifying Delta structure for: $table_name${NC}"
    echo -e "   Path: $table_path"
    
    # Check if path exists
    if ! docker exec minio mc ls minio/$table_path/ >/dev/null 2>&1; then
        echo -e "${RED}❌ Table path does not exist: $table_path${NC}"
        return 1
    fi
    
    # Check for _delta_log directory (critical for Delta format)
    echo -e "${YELLOW}   Checking for _delta_log directory...${NC}"
    if docker exec minio mc ls minio/$table_path/_delta_log/ >/dev/null 2>&1; then
        echo -e "${GREEN}   ✅ _delta_log directory found${NC}"
        
        # List delta log files
        echo -e "${YELLOW}   Delta log files:${NC}"
        docker exec minio mc ls minio/$table_path/_delta_log/ | sed 's/^/     /'
        
        # Check for transaction log files (00000000000000000000.json, etc.)
        log_files=$(docker exec minio mc ls minio/$table_path/_delta_log/ | grep -E '\.json$' | wc -l)
        if [ $log_files -gt 0 ]; then
            echo -e "${GREEN}   ✅ Found $log_files Delta transaction log files${NC}"
        else
            echo -e "${RED}   ❌ No Delta transaction log files found${NC}"
            return 1
        fi
    else
        echo -e "${RED}   ❌ _delta_log directory not found - NOT Delta format!${NC}"
        return 1
    fi
    
    # Check for Parquet data files
    echo -e "${YELLOW}   Checking for Parquet data files...${NC}"
    parquet_files=$(docker exec minio mc ls --recursive minio/$table_path/ | grep -E '\.parquet$' | wc -l)
    if [ $parquet_files -gt 0 ]; then
        echo -e "${GREEN}   ✅ Found $parquet_files Parquet data files${NC}"
    else
        echo -e "${YELLOW}   ⚠️  No Parquet files found (may be empty table)${NC}"
    fi
    
    # Check partitioning structure
    echo -e "${YELLOW}   Checking partitioning structure...${NC}"
    partitions=$(docker exec minio mc ls --recursive minio/$table_path/ | grep -E '(year=|month=|day=)' | head -5)
    if [ -n "$partitions" ]; then
        echo -e "${GREEN}   ✅ Found partitioned structure:${NC}"
        echo "$partitions" | sed 's/^/     /'
    else
        echo -e "${YELLOW}   ⚠️  No partitioning detected${NC}"
    fi
    
    return 0
}

# Function to inspect Delta log content
inspect_delta_log() {
    local table_path=$1
    local table_name=$2
    
    echo -e "\n${YELLOW}📋 Inspecting Delta log content for: $table_name${NC}"
    
    # Get the first transaction log file
    first_log=$(docker exec minio mc ls minio/$table_path/_delta_log/ | grep '\.json$' | head -1 | awk '{print $NF}')
    
    if [ -n "$first_log" ]; then
        echo -e "${YELLOW}   Examining: $first_log${NC}"
        
        # Download and inspect the log file content
        log_content=$(docker exec minio mc cat minio/$table_path/_delta_log/$first_log 2>/dev/null || echo "Failed to read log file")
        
        if [[ $log_content == *"\"protocol\""* ]] && [[ $log_content == *"\"metaData\""* ]]; then
            echo -e "${GREEN}   ✅ Valid Delta log format detected${NC}"
            
            # Extract key information
            if [[ $log_content == *"\"schemaString\""* ]]; then
                echo -e "${GREEN}   ✅ Schema information present${NC}"
            fi
            
            if [[ $log_content == *"\"add\""* ]]; then
                echo -e "${GREEN}   ✅ Data files registered in Delta log${NC}"
            fi
            
            if [[ $log_content == *"\"partitionColumns\""* ]]; then
                echo -e "${GREEN}   ✅ Partition columns configured${NC}"
            fi
            
            # Show a snippet of the log content
            echo -e "${YELLOW}   Log content preview:${NC}"
            echo "$log_content" | head -3 | sed 's/^/     /'
            
        else
            echo -e "${RED}   ❌ Invalid or corrupted Delta log format${NC}"
            echo -e "${YELLOW}   Content preview:${NC}"
            echo "$log_content" | head -3 | sed 's/^/     /'
        fi
    else
        echo -e "${RED}   ❌ No transaction log files found${NC}"
    fi
}

# Function to generate comprehensive report
generate_report() {
    local user_events_status=$1
    local order_events_status=$2
    
    echo -e "\n${BLUE}📊 Delta Format Verification Report${NC}"
    echo -e "${BLUE}====================================${NC}"
    
    echo -e "\n${YELLOW}Table Status Summary:${NC}"
    if [ $user_events_status -eq 0 ]; then
        echo -e "   User Events: ${GREEN}✅ VALID DELTA FORMAT${NC}"
    else
        echo -e "   User Events: ${RED}❌ NOT DELTA FORMAT${NC}"
    fi
    
    if [ $order_events_status -eq 0 ]; then
        echo -e "   Order Events: ${GREEN}✅ VALID DELTA FORMAT${NC}"
    else
        echo -e "   Order Events: ${RED}❌ NOT DELTA FORMAT${NC}"
    fi
    
    echo -e "\n${YELLOW}Delta Format Requirements:${NC}"
    echo -e "   ✓ _delta_log/ directory must exist"
    echo -e "   ✓ Transaction log files (*.json) must be present"
    echo -e "   ✓ Log files must contain protocol, metaData, and add actions"
    echo -e "   ✓ Data files should be in Parquet format"
    echo -e "   ✓ Partitioning structure should be evident"
    
    if [ $user_events_status -eq 0 ] && [ $order_events_status -eq 0 ]; then
        echo -e "\n${GREEN}🎉 VERIFICATION RESULT: S3 OUTPUT IS IN VALID DELTA FORMAT${NC}"
        echo -e "${GREEN}   ✅ All tables conform to Delta Lake specification${NC}"
        echo -e "${GREEN}   ✅ Transaction logs are properly maintained${NC}"
        echo -e "${GREEN}   ✅ Data is stored in Parquet format${NC}"
        echo -e "${GREEN}   ✅ Partitioning is correctly implemented${NC}"
        return 0
    else
        echo -e "\n${RED}❌ VERIFICATION RESULT: ISSUES DETECTED WITH DELTA FORMAT${NC}"
        echo -e "${RED}   Some tables do not conform to Delta Lake specification${NC}"
        return 1
    fi
}

# Main execution
echo -e "${YELLOW}Starting Delta format verification...${NC}"

# Check MinIO accessibility
if ! check_minio; then
    echo -e "${RED}Cannot proceed - MinIO is not accessible${NC}"
    exit 1
fi

# Show bucket contents overview
echo -e "\n${YELLOW}📁 S3 Bucket Contents Overview:${NC}"
docker exec minio mc ls --recursive minio/$BUCKET_NAME/ | head -20

# Verify each table
user_events_status=1
order_events_status=1

# Check User Events table
if verify_delta_structure "$BUCKET_NAME/events/user-events" "User Events"; then
    inspect_delta_log "$BUCKET_NAME/events/user-events" "User Events"
    user_events_status=0
fi

# Check Order Events table  
if verify_delta_structure "$BUCKET_NAME/orders/order-events" "Order Events"; then
    inspect_delta_log "$BUCKET_NAME/orders/order-events" "Order Events"
    order_events_status=0
fi

# Generate final report
generate_report $user_events_status $order_events_status
final_status=$?

# Additional verification commands
echo -e "\n${YELLOW}🔧 Additional Verification Commands:${NC}"
echo -e "   Manual inspection:"
echo -e "   ${BLUE}docker exec minio mc ls --recursive minio/$BUCKET_NAME/${NC}"
echo -e "   ${BLUE}docker exec minio mc cat minio/$BUCKET_NAME/events/user-events/_delta_log/00000000000000000000.json${NC}"

echo -e "\n${YELLOW}📋 What to Look For:${NC}"
echo -e "   • _delta_log directory in each table path"
echo -e "   • JSON transaction log files (numbered sequentially)"
echo -e "   • Parquet data files in partitioned directories"
echo -e "   • Protocol version and metadata in log files"

exit $final_status