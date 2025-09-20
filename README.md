# Calendar Database Pipeline

Generates realistic calendar data (users, events, attendees) for testing and development.

## Quick Start

```bash
# Generate data
uv run python tara/calendar_db_generator/pipeline.py

# Run tests
cd tara/tests && ./run_tests.sh
```

## What It Does

- Generates 50 realistic user profiles
- Creates work and personal calendars
- Generates 15K+ events (meetings, personal events, recurring events)
- Creates 3K+ attendee records with realistic RSVP responses
- Outputs MySQL-compatible SQL with dynamic date variables

## Files

- `tara/calendar_db_generator/pipeline.py` - Main data generation
- `tara/tests/run_tests.sh` - Run all tests
- `output/` - Generated CSV files
- `calendar_data_final.sql` - Final SQL file

## Requirements

- Python 3.11+
- Docker (for MySQL tests)
- `uv` package manager
