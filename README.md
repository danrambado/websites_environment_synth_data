# Calendar Database Pipeline

Generates realistic calendar data (users, events, attendees) for testing and development.

## Setup

1. **Install dependencies:**
   ```bash
   uv install
   ```

2. **Create environment file:**
   ```bash
   # Create .env file in project root
   echo "END_POINT=https://litellm.ml.scaleinternal.com/" > .env
   echo "OPENAI_API_KEY=your_api_key_here" >> .env
   ```
   
   **Required Environment Variables:**
   - `END_POINT`: LiteLLM endpoint URL
   - `OPENAI_API_KEY`: Your OpenAI API key

## Quick Start

```bash
# Generate data
uv run python tara/calendar_db_generator/pipeline.py

# Run tests
uv run pytest tara/tests/test_calendar_pipeline.py -v
```

## What It Does

- Generates 50 realistic user profiles
- Creates work and personal calendars
- Generates 15K+ events (meetings, personal events, recurring events)
- Creates 3K+ attendee records with realistic RSVP responses
- Outputs SQLite-compatible SQL with dynamic date variables

## Testing

The test suite includes comprehensive validation:

```bash
# Run all tests
uv run pytest tara/tests/test_calendar_pipeline.py -v

# Run specific test
uv run pytest tara/tests/test_calendar_pipeline.py::TestCalendarPipeline::test_basic_data_integrity -v
```

**Test Coverage:**
- Data integrity validation
- Foreign key constraints
- Business logic validation
- Dynamic date calculations
- Session consistency
- Data quality metrics

## Files

- `tara/calendar_db_generator/pipeline.py` - Main data generation
- `tara/tests/test_calendar_pipeline.py` - Comprehensive test suite
- `output/` - Generated CSV files
- `calendar_data_final.sql` - Final SQLite-compatible SQL file
- `.env` - Environment variables (create this file)

## Requirements

- Python 3.11+
- `uv` package manager
- OpenAI API key (for AI-generated content)
