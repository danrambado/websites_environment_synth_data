"""
MySQL-based test for Calendar Database Pipeline
Uses Docker MySQL container for testing with pytest
"""

import pytest
import mysql.connector
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


class MySQLPipelineTest:
    """Test the calendar database pipeline with MySQL"""
    
    def __init__(self, sql_file: str = "calendar_data_final.sql"):
        self.sql_file = sql_file
        self.test_today = "2024-01-15"
        self.test_session_id = f"test_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.connection = None
        
    def log(self, message: str):
        """Log a message with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"🔧 {message}")
    
    def setup_mysql_connection(self):
        """Setup MySQL connection"""
        self.log("Setting up MySQL connection...")
        
        # MySQL connection parameters
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'testpassword',
            'database': 'calendar_test',
            'autocommit': True
        }
        
        # Try to connect with retries
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.connection = mysql.connector.connect(**config)
                print("✅ MySQL connection established")
                return
            except mysql.connector.Error as e:
                if attempt < max_retries - 1:
                    print(f"⏳ MySQL connection attempt {attempt + 1} failed: {e}")
                    time.sleep(2)
                else:
                    print(f"❌ Failed to connect to MySQL after {max_retries} attempts: {e}")
                    raise
    
    def create_database(self):
        """Create test database"""
        self.log("Creating test database...")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("DROP DATABASE IF EXISTS calendar_test")
            cursor.execute("CREATE DATABASE calendar_test")
            cursor.execute("USE calendar_test")
            print("✅ Test database created")
        except mysql.connector.Error as e:
            print(f"❌ Error creating database: {e}")
            raise
        finally:
            cursor.close()
    
    def create_schema(self):
        """Create database schema from MySQL schema file"""
        self.log("Creating database schema...")
        
        schema_file = Path(__file__).parent.parent.parent / "seed_data" / "db_schema_mysql.sql"
        try:
            with open(schema_file, 'r') as f:
                schema_content = f.read()
            
            cursor = self.connection.cursor()
            # Split and execute schema statements
            statements = [stmt.strip() for stmt in schema_content.split(';') if stmt.strip()]
            for statement in statements:
                if statement:
                    cursor.execute(statement)
            cursor.close()
            print("✅ Database schema created")
        except Exception as e:
            print(f"❌ Error creating schema: {e}")
            raise
    
    def execute_sql_file(self):
        """Execute the SQL file with MySQL variables"""
        self.log(f"Setting test variables: TODAY={self.test_today}, session_id={self.test_session_id}")
        
        # Read the SQL file
        with open(self.sql_file, 'r') as f:
            sql_content = f.read()
        
        # Replace the example variables with actual test values
        sql_content = sql_content.replace("SET @TODAY = '2024-01-15';", f"SET @TODAY = '{self.test_today}';")
        sql_content = sql_content.replace("SET @session_id = 'session_123';", f"SET @session_id = '{self.test_session_id}';")
        
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
        self.log("Executing SQL file...")
        cursor = self.connection.cursor()
        
        for i, statement in enumerate(statements):
            if statement:
                try:
                    cursor.execute(statement)
                except mysql.connector.Error as e:
                    print(f"❌ Error executing statement {i+1}: {e}")
                    print(f"Statement: {statement[:100]}...")
                    raise
        
        cursor.close()
        print("✅ SQL file executed successfully")
    
    def validate_data(self) -> Dict[str, Any]:
        """Validate the inserted data"""
        self.log("Validating data...")
        
        cursor = self.connection.cursor()
        results = {}
        
        try:
            # Count records in each table
            tables = ['users', 'calendars', 'events', 'attendees']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                results[table] = count
                print(f"✅ {table}: {count} records")
            
            # Validate data integrity
            self.log("Validating data integrity...")
            
            # Check foreign key constraints
            cursor.execute("""
                SELECT COUNT(*) FROM events e 
                WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = e.user_id)
            """)
            invalid_user_refs = cursor.fetchone()[0]
            assert invalid_user_refs == 0, f"Found {invalid_user_refs} events with invalid user references"
            
            cursor.execute("""
                SELECT COUNT(*) FROM events e 
                WHERE NOT EXISTS (SELECT 1 FROM calendars c WHERE c.id = e.calendar_id)
            """)
            invalid_calendar_refs = cursor.fetchone()[0]
            assert invalid_calendar_refs == 0, f"Found {invalid_calendar_refs} events with invalid calendar references"
            
            cursor.execute("""
                SELECT COUNT(*) FROM attendees a 
                WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = a.event_id)
            """)
            invalid_event_refs = cursor.fetchone()[0]
            assert invalid_event_refs == 0, f"Found {invalid_event_refs} attendees with invalid event references"
            
            print("✅ All foreign key constraints satisfied")
            
            # Check session_id consistency
            cursor.execute("SELECT DISTINCT session_id FROM events")
            event_sessions = [row[0] for row in cursor.fetchall()]
            assert len(event_sessions) == 1, f"All events should have same session_id, found: {event_sessions}"
            
            cursor.execute("SELECT DISTINCT session_id FROM attendees")
            attendee_sessions = [row[0] for row in cursor.fetchall()]
            assert len(attendee_sessions) == 1, f"All attendees should have same session_id, found: {attendee_sessions}"
            
            print(f"✅ Session ID consistency: {event_sessions[0]}")
            
        finally:
            cursor.close()
        
        return results
    
    def generate_test_report(self, results: Dict[str, Any]):
        """Generate a test report"""
        self.log("Generating test report...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'test_today': self.test_today,
            'test_session_id': self.test_session_id,
            'results': results,
            'status': 'PASSED'
        }
        
        # Save report
        report_file = f"/tmp/test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"📄 Test report saved: {report_file}")
        return report
    
    def cleanup(self):
        """Clean up resources"""
        if self.connection:
            self.connection.close()
            print("🔌 MySQL connection closed")


# Pytest fixtures and tests
@pytest.fixture(scope="session")
def mysql_test_instance():
    """Create MySQL test instance"""
    sql_file = Path(__file__).parent.parent.parent / "calendar_data_final.sql"
    test_instance = MySQLPipelineTest(str(sql_file))
    
    # Setup
    test_instance.setup_mysql_connection()
    test_instance.create_database()
    test_instance.create_schema()
    test_instance.execute_sql_file()
    
    yield test_instance
    
    # Cleanup
    test_instance.cleanup()


@pytest.fixture(scope="session")
def test_results(mysql_test_instance):
    """Get test results from MySQL test instance"""
    return mysql_test_instance.validate_data()


class TestMySQLPipeline:
    """Test class for MySQL pipeline tests"""
    
    def test_basic_data_integrity(self, test_results):
        """Test basic data integrity"""
        assert test_results['users'] > 0, "Should have users"
        assert test_results['calendars'] > 0, "Should have calendars"
        assert test_results['events'] > 0, "Should have events"
        assert test_results['attendees'] > 0, "Should have attendees"
        
        print(f"✅ Basic data integrity: {test_results}")
    
    def test_record_counts(self, test_results):
        """Test that we have reasonable record counts"""
        assert test_results['users'] >= 50, f"Expected at least 50 users, got {test_results['users']}"
        assert test_results['calendars'] >= 100, f"Expected at least 100 calendars, got {test_results['calendars']}"
        assert test_results['events'] >= 1000, f"Expected at least 1000 events, got {test_results['events']}"
        assert test_results['attendees'] >= 1000, f"Expected at least 1000 attendees, got {test_results['attendees']}"
        
        print(f"✅ Record counts validated: {test_results}")
    
    def test_data_relationships(self, mysql_test_instance):
        """Test data relationships and foreign keys"""
        cursor = mysql_test_instance.connection.cursor()
        
        try:
            # Test user-event relationships
            cursor.execute("""
                SELECT COUNT(*) FROM events e 
                WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = e.user_id)
            """)
            invalid_user_refs = cursor.fetchone()[0]
            assert invalid_user_refs == 0, f"Found {invalid_user_refs} events with invalid user references"
            
            # Test calendar-event relationships
            cursor.execute("""
                SELECT COUNT(*) FROM events e 
                WHERE NOT EXISTS (SELECT 1 FROM calendars c WHERE c.id = e.calendar_id)
            """)
            invalid_calendar_refs = cursor.fetchone()[0]
            assert invalid_calendar_refs == 0, f"Found {invalid_calendar_refs} events with invalid calendar references"
            
            # Test event-attendee relationships
            cursor.execute("""
                SELECT COUNT(*) FROM attendees a 
                WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = a.event_id)
            """)
            invalid_event_refs = cursor.fetchone()[0]
            assert invalid_event_refs == 0, f"Found {invalid_event_refs} attendees with invalid event references"
            
            print("✅ All data relationships validated")
            
        finally:
            cursor.close()
    
    def test_session_consistency(self, mysql_test_instance):
        """Test session ID consistency"""
        cursor = mysql_test_instance.connection.cursor()
        
        try:
            # Check events session consistency
            cursor.execute("SELECT DISTINCT session_id FROM events")
            event_sessions = [row[0] for row in cursor.fetchall()]
            assert len(event_sessions) == 1, f"All events should have same session_id, found: {event_sessions}"
            
            # Check attendees session consistency
            cursor.execute("SELECT DISTINCT session_id FROM attendees")
            attendee_sessions = [row[0] for row in cursor.fetchall()]
            assert len(attendee_sessions) == 1, f"All attendees should have same session_id, found: {attendee_sessions}"
            
            # Check that events and attendees have the same session_id
            assert event_sessions[0] == attendee_sessions[0], "Events and attendees should have same session_id"
            
            print(f"✅ Session consistency validated: {event_sessions[0]}")
            
        finally:
            cursor.close()
    
    def test_attendee_distribution(self, mysql_test_instance):
        """Test attendee response distribution"""
        cursor = mysql_test_instance.connection.cursor()
        
        try:
            # Check attendee status distribution
            cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM attendees
                GROUP BY status
                ORDER BY count DESC
            """)
            status_counts = dict(cursor.fetchall())
            
            # Should have multiple status types
            assert len(status_counts) > 1, f"Expected multiple attendee statuses, got: {status_counts}"
            
            # Should have some accepted attendees
            assert 'accepted' in status_counts, "Should have accepted attendees"
            assert status_counts['accepted'] > 0, "Should have some accepted attendees"
            
            print(f"✅ Attendee distribution: {status_counts}")
            
        finally:
            cursor.close()
    
    def test_event_types(self, mysql_test_instance):
        """Test that we have both personal and work events"""
        cursor = mysql_test_instance.connection.cursor()
        
        try:
            # Check personal events
            cursor.execute("SELECT COUNT(*) FROM events WHERE calendar_id LIKE '%personal%'")
            personal_count = cursor.fetchone()[0]
            
            # Check work events
            cursor.execute("SELECT COUNT(*) FROM events WHERE calendar_id LIKE '%work%'")
            work_count = cursor.fetchone()[0]
            
            assert personal_count > 0, "Should have personal events"
            assert work_count > 0, "Should have work events"
            
            print(f"✅ Event types: {personal_count} personal, {work_count} work events")
            
        finally:
            cursor.close()


def main():
    """Main function for standalone execution"""
    import sys
    
    # Parse command line arguments
    sql_file = "calendar_data_final.sql"
    if len(sys.argv) > 1:
        if sys.argv[1] == "--sql-file" and len(sys.argv) > 2:
            sql_file = sys.argv[2]
        elif not sys.argv[1].startswith("--"):
            sql_file = sys.argv[1]
    
    test = MySQLPipelineTest(sql_file)
    
    try:
        test.setup_mysql_connection()
        test.create_database()
        test.create_schema()
        test.execute_sql_file()
        results = test.validate_data()
        report = test.generate_test_report(results)
        
        print("\n" + "=" * 50)
        print("📊 TEST SUMMARY")
        print("=" * 50)
        for table, count in results.items():
            print(f"✅ {table.capitalize()}: {count}")
        print(f"📊 Total Records: {sum(results.values())}")
        print("🎉 All validations passed!")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        return False
        
    finally:
        test.cleanup()


if __name__ == "__main__":
    success = main()
    if not success:
        import sys
        sys.exit(1)
