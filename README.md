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

## Pipeline Execution

The pipeline has 7 steps with checkpoint support:

```bash
# Full pipeline (requires LLM calls)
uv run python tara/calendar_db_generator/pipeline.py

# Skip expensive LLM steps (use cached data)
uv run python tara/calendar_db_generator/pipeline.py --skip profiles personal_events

# Generate only work events (no LLM needed)
uv run python tara/calendar_db_generator/pipeline.py --skip profiles personal_events attendees

# Force re-run specific steps
uv run python tara/calendar_db_generator/pipeline.py --force profiles
```

**Pipeline Steps:**
1. **`profiles`** - Generate realistic user profiles with personal/professional details using AI *(requires LLM)*
2. **`dataframes`** - Create users and calendars DataFrames from profiles *(local processing)*
3. **`work_events`** - Generate work meetings using predefined templates *(local processing)*
4. **`personal_events`** - Generate personal events using AI based on user profiles *(requires LLM)*
5. **`attendees`** - Generate attendee records with realistic RSVP responses using algorithmic rules *(local processing)*
6. **`sql`** - Generate SQLite-compatible SQL file with INSERT statements *(local processing)*
7. **`execute_sql`** - Execute SQL file on SQLite database with variable replacement *(local processing)*

**LLM-Required Steps:** `profiles`, `personal_events`  
**Local Processing Steps:** `dataframes`, `work_events`, `attendees`, `sql`, `execute_sql`

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
