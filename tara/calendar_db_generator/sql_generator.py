#!/usr/bin/env python3
"""
Dynamic SQL Generator with TODAY and session_id variables
Generates SQL file with schema and inserts for calendar database (SQLite)
"""

import pandas as pd
from datetime import datetime
from typing import List
import os
import sqlite3
import re

class SQLGenerator:
    def __init__(self, today_override=None):
        # Allow overriding TODAY for testing
        self.today = today_override if today_override else datetime.now().date()
        self.today_str = self.today.strftime('%Y-%m-%d')
        
        # CSV files to process (matching pipeline output)
        self.csv_files = {
            'users': 'output/users.csv',
            'calendars': 'output/calendars.csv', 
            'events': 'output/events.csv',
            'attendees': 'output/attendees.csv'
        }
        
        # Schema file
        self.schema_file = 'seed_data/db_schema.sql'  # Use SQLite schema
        
        print(f"🗓️  TODAY: {self.today} ({self.today.strftime('%A')})")
        print(f"📅 SQL will adjust TODAY to match original event weekdays")
    
    def _safe_parse_datetime(self, date_str: str) -> datetime:
        """Safely parse datetime string, fixing common format issues"""
        try:
            return datetime.fromisoformat(date_str)
        except ValueError:
            # Fix common date format issues
            import re
            
            # Fix missing zero padding in day (2024-02-5T -> 2024-02-05T)
            fixed_date = re.sub(r'(\d{4}-\d{1,2})-(\d{1,2})T', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}T", date_str)
            
            # Fix missing zero padding in month (2024-2-05T -> 2024-02-05T)
            fixed_date = re.sub(r'(\d{4})-(\d{1,2})-(\d{2})T', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3)}T", fixed_date)
            
            try:
                return datetime.fromisoformat(fixed_date)
            except ValueError:
                # If still invalid, return current time
                print(f"⚠️  Warning: Invalid date format '{date_str}', using current time")
                return datetime.now()
    
    def generate_inserts(self, table_name: str, csv_file: str) -> List[str]:
        """Generate INSERT statements with TODAY and session_id variable support"""
        try:
            df = pd.read_csv(csv_file)
            inserts = []
            
            for _, row in df.iterrows():
                if table_name == 'events':
                    # For events, we'll adjust TODAY to match the original event's weekday
                    start_time = self._safe_parse_datetime(row['start_time'])
                    end_time = self._safe_parse_datetime(row['end_time'])
                    
                    # Get original weekdays and convert to SQLite format
                    # Python: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday, 5=Saturday, 6=Sunday
                    # SQLite: 0=Sunday, 1=Monday, 2=Tuesday, 3=Wednesday, 4=Thursday, 5=Friday, 6=Saturday
                    python_weekday = start_time.weekday()
                    original_start_weekday = (python_weekday + 1) % 7  # Convert to SQLite format
                    
                    python_weekday = end_time.weekday()
                    original_end_weekday = (python_weekday + 1) % 7  # Convert to SQLite format
                    
                    # Generate SQL with weekday-aware date calculations using TODAY variable
                    # Exclude columns that SQLite will auto-generate or don't exist in schema
                    columns_to_use = [col for col in df.columns if col not in ['id', 'unique_event_id']]
                    columns = ', '.join(columns_to_use)
                    
                    values = []
                    for col in columns_to_use:
                        if col == 'start_time':
                            time_part = start_time.strftime('%H:%M:%S')
                            # Use SQLite datetime function for dynamic date calculation
                            # Calculate days to add: (original_weekday - current_weekday + 7) % 7
                            values.append(f"datetime(@TODAY, '+{((original_start_weekday - self.today.weekday() + 7) % 7)} days', '{time_part}')")
                        elif col == 'end_time':
                            time_part = end_time.strftime('%H:%M:%S')
                            # Use SQLite datetime function for dynamic date calculation
                            values.append(f"datetime(@TODAY, '+{((original_end_weekday - self.today.weekday() + 7) % 7)} days', '{time_part}')")
                        elif col == 'session_id':
                            # Use placeholder for session_id (will be replaced later)
                            values.append('@session_id')
                        else:
                            value = row[col]
                            if pd.isna(value):
                                values.append('NULL')
                            elif isinstance(value, str):
                                escaped_value = value.replace("'", "''").replace("\\", "\\\\")
                                values.append(f"'{escaped_value}'")
                            else:
                                values.append(str(value))
                    
                    values_str = ', '.join(values)
                    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str});"
                    inserts.append(insert_sql)
                else:
                    # For non-event tables, use regular INSERT with session_id variable
                    # Handle different table types: calendars has TEXT primary key, others use AUTOINCREMENT
                    if table_name == 'calendars':
                        # Calendars table: include 'id' field (TEXT primary key)
                        columns_to_use = [col for col in df.columns if col not in ['unique_event_id']]
                    else:
                        # Users, attendees tables: exclude 'id' field (AUTOINCREMENT)
                        columns_to_use = [col for col in df.columns if col not in ['id', 'unique_event_id']]
                    columns = ', '.join(columns_to_use)
                    
                    values = []
                    for col in columns_to_use:
                        if col == 'session_id':
                            # Use placeholder for session_id (will be replaced later)
                            values.append('@session_id')
                        else:
                            value = row[col]
                            if pd.isna(value):
                                values.append('NULL')
                            elif isinstance(value, str):
                                escaped_value = value.replace("'", "''").replace("\\", "\\\\")
                                values.append(f"'{escaped_value}'")
                            else:
                                values.append(str(value))
                    
                    values_str = ', '.join(values)
                    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str});"
                    inserts.append(insert_sql)
            
            return inserts
            
        except Exception as e:
            print(f"❌ Error processing {csv_file}: {e}")
            return []
    
    def replace_variables(self, sql_content: str, today: str, session_id: str) -> str:
        """Replace @TODAY and @session_id variables with actual values"""
        # Replace @TODAY with actual date
        sql_content = sql_content.replace('@TODAY', f"'{today}'")
        # Replace @session_id with actual session ID
        sql_content = sql_content.replace('@session_id', f"'{session_id}'")
        return sql_content

    def generate_sql(self, output_file: str = "calendar_data_final.sql"):
        """Generate SQL file with schema and INSERT statements using @TODAY and @session_id variables"""
        print(f"\n🔄 Generating SQL file...")
        
        with open(output_file, 'w') as f:
            # Write header with SQLite variables at the very top
            f.write(f"""-- Calendar Database SQL File (SQLite)
-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- 
-- Variables to be replaced before running:
-- @TODAY = '2024-01-15'
-- @session_id = 'session_123'
--
-- Example usage:
-- Replace @TODAY with actual date: '2024-01-15'
-- Replace @session_id with actual session ID: 'session_123'
-- [rest of SQL statements]
--
-- This file contains INSERT statements only:
-- 1. INSERT statements for users, calendars, events
-- 2. Temporary table to map events to their auto-generated IDs
-- 3. INSERT statements for attendees with correct event_id references
-- 4. Dynamic date calculations using @TODAY variable
--

""")
            
            # Phase 1: Insert users, calendars, and events
            f.write("-- PHASE 1: Insert users, calendars, and events\n")
            f.write("-- ==============================================\n\n")
            
            for table_name in ['users', 'calendars', 'events']:
                csv_file = self.csv_files[table_name]
                if os.path.exists(csv_file):
                    print(f"📄 Processing {table_name} from {csv_file}")
                    inserts = self.generate_inserts(table_name, csv_file)
                    if inserts:
                        f.write(f"-- {table_name.upper()} DATA\n")
                        for insert in inserts:
                            f.write(insert + "\n")
                        f.write("\n")
                    else:
                        print(f"⚠️  No inserts generated for {table_name}")
                else:
                    print(f"⚠️  CSV file not found: {csv_file}")
            
            # Phase 2: Create temporary mapping table and insert attendees
            f.write("-- PHASE 2: Create event mapping and insert attendees\n")
            f.write("-- ===================================================\n\n")
            
            f.write("""-- Create temporary table to map events to their auto-generated IDs
CREATE TEMPORARY TABLE event_mapping AS
SELECT 
    id as event_id,
    user_id,
    title,
    start_time,
    end_time,
    calendar_id
FROM events
ORDER BY id;

""")
            
            # Generate attendees with event_id mapping
            if os.path.exists(self.csv_files['attendees']):
                print(f"📄 Processing attendees with event mapping")
                attendee_inserts = self.generate_attendees_with_mapping()
                if attendee_inserts:
                    f.write("-- ATTENDEES DATA (with event_id mapping)\n")
                    for insert in attendee_inserts:
                        f.write(insert + "\n")
                    f.write("\n")
                else:
                    print(f"⚠️  No attendee inserts generated")
            else:
                print(f"⚠️  Attendees CSV file not found: {self.csv_files['attendees']}")
            
            print(f"✅ SQL file generated: {output_file}")

    def generate_attendees_with_mapping(self) -> List[str]:
        """Generate attendees INSERT statements using event mapping table"""
        try:
            df = pd.read_csv(self.csv_files['attendees'])
            inserts = []
            
            for _, row in df.iterrows():
                user_id = row['user_id']
                event_user_id = row['event_user_id']
                event_title = row['event_title']
                status = row['status']
                responded_at = row['responded_at'] if not pd.isna(row['responded_at']) else 'NULL'
                
                # Match attendees to events using user_id and title
                insert_sql = f"""INSERT INTO attendees (event_id, user_id, status, responded_at, session_id)
SELECT 
    em.event_id,
    {user_id},
    '{status}',
    {responded_at if responded_at != 'NULL' and responded_at.startswith('datetime(') else f"'{responded_at}'" if responded_at != 'NULL' else 'NULL'},
    @session_id
FROM event_mapping em
WHERE em.user_id = {event_user_id}
  AND em.title = '{event_title.replace("'", "''")}'
LIMIT 1;"""
                
                inserts.append(insert_sql)
            
            return inserts
            
        except Exception as e:
            print(f"❌ Error processing attendees: {e}")
            return []

    def execute_sql_file(self, sql_file: str, db_file: str, today: str = None, session_id: str = None):
        """Execute SQL file with variable replacement on SQLite database"""
        if today is None:
            today = self.today_str
        if session_id is None:
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print(f"🔄 Executing SQL file with TODAY={today}, session_id={session_id}")
        
        # Read SQL file
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Replace variables
        sql_content = self.replace_variables(sql_content, today, session_id)
        
        # Connect to SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        try:
            # Split SQL content into individual statements
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
            for i, statement in enumerate(statements):
                if statement and not statement.startswith('--'):
                    try:
                        cursor.execute(statement)
                    except sqlite3.Error as e:
                        print(f"❌ Error executing statement {i+1}: {e}")
                        print(f"Statement: {statement[:100]}...")
                        raise
            
            conn.commit()
            print(f"✅ SQL file executed successfully on {db_file}")
            
        finally:
            cursor.close()
            conn.close()

def main():
    generator = SQLGenerator()
    generator.generate_sql()

if __name__ == "__main__":
    main()
