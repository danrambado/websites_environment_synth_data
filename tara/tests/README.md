# Calendar Database Pipeline - Tests

## Run Tests
```bash
cd tara/tests
./run_tests.sh
```

## Prerequisites
1. Generate data first: `uv run python tara/calendar_db_generator/pipeline.py`
2. Docker running
3. Dependencies installed: `uv install`

## What Tests Run
- **Phase 1**: Data integrity & business logic tests
- **Phase 2**: MySQL integration tests  
- **Phase 3**: Data quality validation

## Files
- `run_tests.sh` - Main test runner
- `test_calendar_pipeline.py` - Comprehensive tests
- `test_mysql_pipeline.py` - MySQL tests
- `docker-compose.yml` - MySQL container

## Troubleshooting
- **MySQL issues**: Ensure Docker is running, wait up to 60s for startup
- **Missing data**: Run pipeline first to generate `calendar_data_final.sql`
- **Test failures**: Check pytest output, verify dependencies
