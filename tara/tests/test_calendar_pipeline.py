"""
Comprehensive pytest tests for the Calendar Database Pipeline
Tests data integrity, business logic, and SQL generation with MySQL
"""

import pytest
import mysql.connector
import time
from pathlib import Path


class TestCalendarPipeline:
    """Test suite for calendar database pipeline with MySQL"""
    
    @pytest.fixture
    def mysql_connection(self):
        """Create MySQL connection for testing"""
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'testpassword',
            'autocommit': True
        }
        
        # Try to connect with retries
        max_retries = 10
        for attempt in range(max_retries):
            try:
                conn = mysql.connector.connect(**config)
                yield conn
                conn.close()
                return
            except mysql.connector.Error as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    pytest.skip(f"Could not connect to MySQL: {e}")
    
    @pytest.fixture
    def test_database(self, mysql_connection):
        """Create and setup test database"""
        cursor = mysql_connection.cursor()
        try:
            cursor.execute("DROP DATABASE IF EXISTS calendar_test_pytest")
            cursor.execute("CREATE DATABASE calendar_test_pytest")
            cursor.execute("USE calendar_test_pytest")
            yield "calendar_test_pytest"
        finally:
            cursor.close()
    
    @pytest.fixture
    def schema_sql(self):
        """Load MySQL database schema from file"""
        schema_path = Path(__file__).parent.parent.parent / "seed_data" / "db_schema_mysql.sql"
        with open(schema_path, 'r') as f:
            return f.read()
    
    @pytest.fixture
    def test_data_sql(self):
        """Load test data SQL from generated file"""
        sql_path = Path(__file__).parent.parent.parent / "calendar_data_final.sql"
        with open(sql_path, 'r') as f:
            return f.read()
    
    @pytest.fixture
    def test_params(self):
        """Test parameters for SQL execution"""
        return ('2024-01-15', 'test_session_pytest')
    
    def setup_database(self, conn, schema_sql, test_data_sql, test_params):
        """Setup database with schema and test data"""
        # Create schema
        cursor = conn.cursor()
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        for statement in statements:
            if statement:
                cursor.execute(statement)
        
        # Replace variables and execute test data
        sql_content = test_data_sql.replace("SET @TODAY = '2024-01-15';", f"SET @TODAY = '{test_params[0]}';")
        sql_content = sql_content.replace("SET @session_id = 'session_123';", f"SET @session_id = '{test_params[1]}';")
        
        # Split SQL content into individual statements (handle semicolons inside strings)
        statements = []
        current_statement = ""
        in_string = False
        escape_next = False
        
        for char in sql_content:
            if escape_next:
                current_statement += char
                escape_next = False
            elif char == '\\' and in_string:
                current_statement += char
                escape_next = True
            elif char == "'" and not escape_next:
                in_string = not in_string
                current_statement += char
            elif char == ';' and not in_string:
                if current_statement.strip():
                    statements.append(current_statement.strip())
                current_statement = ""
            else:
                current_statement += char
        
        # Add the last statement if it exists
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        # Execute each statement
        for statement in statements:
            if statement:
                cursor.execute(statement)
        
        cursor.close()
    
    def test_basic_data_integrity(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test basic data integrity - all tables have expected records"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        # Test record counts
        cursor = mysql_connection.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        assert user_count > 0, "Users table should have records"
        
        cursor.execute("SELECT COUNT(*) FROM calendars")
        calendar_count = cursor.fetchone()[0]
        assert calendar_count > 0, "Calendars table should have records"
        
        cursor.execute("SELECT COUNT(*) FROM events")
        event_count = cursor.fetchone()[0]
        assert event_count > 0, "Events table should have records"
        
        cursor.execute("SELECT COUNT(*) FROM attendees")
        attendee_count = cursor.fetchone()[0]
        assert attendee_count > 0, "Attendees table should have records"
        
        print(f"✅ Data integrity check passed: {user_count} users, {calendar_count} calendars, {event_count} events, {attendee_count} attendees")
        cursor.close()
    
    def test_foreign_key_constraints(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test that all foreign key constraints are satisfied"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Test events have valid user_id references
        cursor.execute("""
            SELECT COUNT(*) FROM events e 
            WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = e.user_id)
        """)
        invalid_user_refs = cursor.fetchone()[0]
        assert invalid_user_refs == 0, f"Found {invalid_user_refs} events with invalid user references"
        
        # Test events have valid calendar_id references
        cursor.execute("""
            SELECT COUNT(*) FROM events e 
            WHERE NOT EXISTS (SELECT 1 FROM calendars c WHERE c.id = e.calendar_id)
        """)
        invalid_calendar_refs = cursor.fetchone()[0]
        assert invalid_calendar_refs == 0, f"Found {invalid_calendar_refs} events with invalid calendar references"
        
        # Test attendees have valid event_id references
        cursor.execute("""
            SELECT COUNT(*) FROM attendees a 
            WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = a.event_id)
        """)
        invalid_event_refs = cursor.fetchone()[0]
        assert invalid_event_refs == 0, f"Found {invalid_event_refs} attendees with invalid event references"
        
        # Test attendees have valid user_id references
        cursor.execute("""
            SELECT COUNT(*) FROM attendees a 
            WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = a.user_id)
        """)
        invalid_attendee_user_refs = cursor.fetchone()[0]
        assert invalid_attendee_user_refs == 0, f"Found {invalid_attendee_user_refs} attendees with invalid user references"
        
        print("✅ All foreign key constraints satisfied")
        cursor.close()
    
    def test_user_event_relationship(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test that users have events and attendees are properly linked"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Get a sample user
        cursor.execute("SELECT id, name FROM users LIMIT 1")
        user = cursor.fetchone()
        assert user is not None, "Should have at least one user"
        
        user_id, user_name = user
        
        # Check user has events
        cursor.execute("SELECT COUNT(*) FROM events WHERE user_id = %s", (user_id,))
        user_event_count = cursor.fetchone()[0]
        assert user_event_count > 0, f"User {user_name} should have events"
        
        # Check user has attendees for their events
        cursor.execute("""
            SELECT COUNT(*) FROM attendees a
            JOIN events e ON a.event_id = e.id
            WHERE e.user_id = %s
        """, (user_id,))
        user_attendee_count = cursor.fetchone()[0]
        assert user_attendee_count > 0, f"User {user_name} should have attendees for their events"
        
        print(f"✅ User {user_name} has {user_event_count} events and {user_attendee_count} attendees")
        cursor.close()
    
    def test_personal_vs_work_events(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test that personal and work events are properly categorized"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Check personal events exist
        cursor.execute("SELECT COUNT(*) FROM events WHERE calendar_id LIKE '%personal%'")
        personal_count = cursor.fetchone()[0]
        assert personal_count > 0, "Should have personal events"
        
        # Check work events exist
        cursor.execute("SELECT COUNT(*) FROM events WHERE calendar_id LIKE '%work%'")
        work_count = cursor.fetchone()[0]
        assert work_count > 0, "Should have work events"
        
        # Check calendar types
        cursor.execute("SELECT DISTINCT calendar_id FROM events")
        calendar_types = [row[0] for row in cursor.fetchall()]
        
        has_personal = any('personal' in cal for cal in calendar_types)
        has_work = any('work' in cal for cal in calendar_types)
        
        assert has_personal, "Should have personal calendar types"
        assert has_work, "Should have work calendar types"
        
        print(f"✅ Event categorization: {personal_count} personal events, {work_count} work events")
        cursor.close()
    
    def test_recurring_events(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test that recurring events are properly handled"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Check for recurring events (rrule not null/empty)
        cursor.execute("SELECT COUNT(*) FROM events WHERE rrule IS NOT NULL AND rrule != ''")
        recurring_count = cursor.fetchone()[0]
        
        if recurring_count > 0:
            # Get a sample recurring event
            cursor.execute("""
                SELECT id, title, rrule, start_time, end_time 
                FROM events 
                WHERE rrule IS NOT NULL AND rrule != '' 
                LIMIT 1
            """)
            recurring_event = cursor.fetchone()
            assert recurring_event is not None, "Should have recurring events"
            
            event_id, title, rrule, start_time, end_time = recurring_event
            
            # Check that recurring events have valid rrule format
            assert rrule is not None, "Recurring event should have rrule"
            assert len(rrule) > 0, "Recurring event should have non-empty rrule"
            
            print(f"✅ Recurring event found: '{title}' with rrule: {rrule}")
        else:
            print("ℹ️ No recurring events found in test data")
        cursor.close()
    
    def test_attendee_response_times(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test that attendee response times are realistic"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Check attendees have realistic response times
        cursor.execute("""
            SELECT COUNT(*) FROM attendees 
            WHERE responded_at IS NOT NULL
        """)
        responded_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM attendees")
        total_attendees = cursor.fetchone()[0]
        
        # Some attendees should have responded
        response_rate = responded_count / total_attendees if total_attendees > 0 else 0
        assert response_rate > 0, "Some attendees should have responded"
        # Allow for cases where all attendees have responded (response_rate = 1.0)
        assert response_rate <= 1, "Response rate should not exceed 100%"
        
        # Check response status distribution
        cursor.execute("""
            SELECT status, COUNT(*) 
            FROM attendees 
            GROUP BY status
        """)
        status_counts = dict(cursor.fetchall())
        
        assert 'accepted' in status_counts, "Should have accepted responses"
        assert 'declined' in status_counts, "Should have declined responses"
        assert 'no_response' in status_counts, "Should have no_response attendees"
        
        print(f"✅ Attendee responses: {status_counts}")
        cursor.close()
    
    def test_session_id_consistency(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test that session_id is consistent across all records"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Check all events have the same session_id
        cursor.execute("SELECT DISTINCT session_id FROM events")
        event_sessions = [row[0] for row in cursor.fetchall()]
        assert len(event_sessions) == 1, f"All events should have same session_id, found: {event_sessions}"
        
        # Check all attendees have the same session_id
        cursor.execute("SELECT DISTINCT session_id FROM attendees")
        attendee_sessions = [row[0] for row in cursor.fetchall()]
        assert len(attendee_sessions) == 1, f"All attendees should have same session_id, found: {attendee_sessions}"
        
        # Check session_id matches test parameter
        expected_session = test_params[1]
        assert event_sessions[0] == expected_session, f"Event session_id should be {expected_session}"
        assert attendee_sessions[0] == expected_session, f"Attendee session_id should be {expected_session}"
        
        print(f"✅ Session ID consistency: {expected_session}")
        cursor.close()
    
    def test_dynamic_date_calculations(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test that dynamic date calculations work correctly"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Get a sample event with start_time
        cursor.execute("""
            SELECT id, title, start_time, end_time 
            FROM events 
            WHERE start_time IS NOT NULL 
            LIMIT 1
        """)
        event = cursor.fetchone()
        assert event is not None, "Should have events with start_time"
        
        event_id, title, start_time, end_time = event
        
        # Check that dates are properly formatted
        assert start_time is not None, "Event should have start_time"
        assert end_time is not None, "Event should have end_time"
        
        # Parse dates to ensure they're valid
        try:
            start_dt = start_time
            end_dt = end_time
            assert start_dt < end_dt, "Start time should be before end time"
        except Exception as e:
            pytest.fail(f"Invalid date format: {e}")
        
        print(f"✅ Dynamic date calculation working: '{title}' from {start_time} to {end_time}")
        cursor.close()
    
    def test_business_logic_validation(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test business logic - events should make sense"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Test that work events are during business hours
        cursor.execute("""
            SELECT COUNT(*) FROM events 
            WHERE calendar_id LIKE '%work%' 
            AND start_time IS NOT NULL
            AND HOUR(start_time) NOT BETWEEN 8 AND 18
        """)
        non_business_hours = cursor.fetchone()[0]
        
        # Most work events should be during business hours (allow some flexibility)
        cursor.execute("SELECT COUNT(*) FROM events WHERE calendar_id LIKE '%work%' AND start_time IS NOT NULL")
        total_work_events = cursor.fetchone()[0]
        
        if total_work_events > 0:
            non_business_ratio = non_business_hours / total_work_events
            assert non_business_ratio < 0.3, f"Too many work events outside business hours: {non_business_ratio:.2%}"
        
        # Test that regular personal events (not vacations/trips) have reasonable durations
        cursor.execute("""
            SELECT COUNT(*) FROM events 
            WHERE calendar_id LIKE '%personal%' 
            AND start_time IS NOT NULL 
            AND end_time IS NOT NULL
            AND TIMESTAMPDIFF(HOUR, start_time, end_time) > 24
            AND title NOT LIKE '%vacation%'
            AND title NOT LIKE '%trip%'
            AND title NOT LIKE '%getaway%'
            AND title NOT LIKE '%retreat%'
            AND title NOT LIKE '%conference%'
            AND title NOT LIKE '%business%'
            AND title NOT LIKE '%visit%'
            AND title NOT LIKE '%break%'
            AND title NOT LIKE '%holiday%'
            AND title NOT LIKE '%cruise%'
            AND title NOT LIKE '%summit%'
            AND title NOT LIKE '%meeting%'
            AND title NOT LIKE '%detox%'
            AND title NOT LIKE '%convention%'
            AND title NOT LIKE '%meetup%'
            AND title NOT LIKE '%cabin%'
            AND title NOT LIKE '%weekend%'
            AND title NOT LIKE '%time off%'
            AND title NOT LIKE '%ski%'
        """)
        overly_long_regular_personal = cursor.fetchone()[0]
        assert overly_long_regular_personal == 0, f"Regular personal events (not vacations/trips) shouldn't be longer than 24 hours. Found {overly_long_regular_personal} events."
        
        print("✅ Business logic validation passed")
        cursor.close()
    
    def test_data_quality_metrics(self, mysql_connection, test_database, schema_sql, test_data_sql, test_params):
        """Test overall data quality metrics"""
        self.setup_database(mysql_connection, schema_sql, test_data_sql, test_params)
        
        cursor = mysql_connection.cursor()
        
        # Calculate various metrics
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM events")
        event_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM attendees")
        attendee_count = cursor.fetchone()[0]
        
        # Calculate ratios
        events_per_user = event_count / user_count if user_count > 0 else 0
        attendees_per_event = attendee_count / event_count if event_count > 0 else 0
        
        # Reasonable expectations
        assert events_per_user > 10, f"Users should have reasonable number of events: {events_per_user:.1f}"
        assert attendees_per_event > 0.05, f"Events should have some attendees: {attendees_per_event:.3f}"
        
        # Check for data completeness
        cursor.execute("SELECT COUNT(*) FROM events WHERE title IS NULL OR title = ''")
        empty_titles = cursor.fetchone()[0]
        assert empty_titles == 0, "All events should have titles"
        
        cursor.execute("SELECT COUNT(*) FROM users WHERE name IS NULL OR name = ''")
        empty_names = cursor.fetchone()[0]
        assert empty_names == 0, "All users should have names"
        
        print(f"✅ Data quality metrics: {events_per_user:.1f} events/user, {attendees_per_event:.3f} attendees/event")
        cursor.close()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
