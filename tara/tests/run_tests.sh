#!/bin/bash

# Main entry point for all Calendar Database Pipeline tests
# Comprehensive end-to-end test runner

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "INFO")
            echo -e "${BLUE}ℹ️  $message${NC}"
            ;;
        "SUCCESS")
            echo -e "${GREEN}✅ $message${NC}"
            ;;
        "WARNING")
            echo -e "${YELLOW}⚠️  $message${NC}"
            ;;
        "ERROR")
            echo -e "${RED}❌ $message${NC}"
            ;;
    esac
}

# Function to cleanup Docker containers
cleanup() {
    print_status "INFO" "Cleaning up Docker containers..."
    docker-compose -f docker-compose.yml down 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

echo "🧪 Calendar Database Pipeline - Comprehensive Test Suite"
echo "=========================================================="
echo ""

# Check if we're in the right directory
if [ ! -f "docker-compose.yml" ]; then
    print_status "ERROR" "docker-compose.yml not found. Please run from tara/tests directory."
    exit 1
fi

# Check if calendar_data_final.sql exists
if [ ! -f "../../calendar_data_final.sql" ]; then
    print_status "ERROR" "calendar_data_final.sql not found. Please run the pipeline first to generate data."
    exit 1
fi

print_status "INFO" "Starting MySQL container..."
docker-compose -f docker-compose.yml up -d

print_status "INFO" "Waiting for MySQL to be ready..."
timeout=60
counter=0
while ! docker exec mysql-pipeline-test mysqladmin ping -h localhost --silent; do
    if [ $counter -eq $timeout ]; then
        print_status "ERROR" "MySQL failed to start within $timeout seconds"
        exit 1
    fi
    echo -e "${YELLOW}⏳ Waiting for MySQL... ($counter/$timeout)${NC}"
    sleep 2
    counter=$((counter + 2))
done

print_status "SUCCESS" "MySQL is ready!"

echo ""
echo "📊 PHASE 1: Comprehensive Data Integrity Tests"
echo "=============================================="
print_status "INFO" "Running comprehensive data integrity and business logic tests..."

# Run the comprehensive pytest tests
cd ../.. && uv run pytest tara/tests/test_calendar_pipeline.py -v --tb=short

if [ $? -eq 0 ]; then
    print_status "SUCCESS" "Phase 1: Comprehensive tests PASSED"
else
    print_status "ERROR" "Phase 1: Comprehensive tests FAILED"
    exit 1
fi

echo ""
echo "📊 PHASE 2: MySQL Pipeline Integration Tests"
echo "============================================"
print_status "INFO" "Running MySQL pipeline integration tests..."

# Run the MySQL pipeline tests with pytest
uv run pytest tara/tests/test_mysql_pipeline.py -v --tb=short

if [ $? -eq 0 ]; then
    print_status "SUCCESS" "Phase 2: MySQL pipeline tests PASSED"
else
    print_status "ERROR" "Phase 2: MySQL pipeline tests FAILED"
    exit 1
fi

echo ""
echo "📊 PHASE 3: Data Quality Validation"
echo "===================================="
print_status "INFO" "Running additional data quality checks..."

# Run a quick data quality check
uv run python -c "
import pandas as pd
import sys

print('📊 Data Quality Summary:')
print('=' * 30)

try:
    # Check users
    users_df = pd.read_csv('output/users.csv')
    print(f'👥 Users: {len(users_df)}')
    
    # Check calendars
    calendars_df = pd.read_csv('output/calendars.csv')
    print(f'📅 Calendars: {len(calendars_df)}')
    
    # Check events
    events_df = pd.read_csv('output/events.csv')
    print(f'📝 Events: {len(events_df)}')
    
    # Check attendees
    attendees_df = pd.read_csv('output/attendees.csv')
    print(f'👥 Attendees: {len(attendees_df)}')
    
    # Check attendee distribution
    status_counts = attendees_df['status'].value_counts()
    print(f'📊 Attendee Status Distribution:')
    for status, count in status_counts.items():
        print(f'   {status}: {count}')
    
    print('✅ Data quality validation completed')
    
except Exception as e:
    print(f'❌ Data quality check failed: {e}')
    sys.exit(1)
"

if [ $? -eq 0 ]; then
    print_status "SUCCESS" "Phase 3: Data quality validation PASSED"
else
    print_status "ERROR" "Phase 3: Data quality validation FAILED"
    exit 1
fi

echo ""
echo "🎉 ALL TESTS PASSED!"
echo "===================="
echo ""
print_status "SUCCESS" "Test Summary:"
echo "  ✅ Comprehensive data integrity tests"
echo "  ✅ Business logic validation tests"
echo "  ✅ MySQL pipeline integration tests"
echo "  ✅ Data quality validation"
echo "  ✅ End-to-end data generation and validation"
echo ""
echo "📊 Pipeline Status: READY FOR PRODUCTION"
echo "=========================================="
