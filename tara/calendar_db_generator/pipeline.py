import sys
import pandas as pd
import os
import json
import pickle
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from tara.lib.pipeline import Pipeline
from tara.calendar_db_generator.profile_generator import ProfileGenerator
from tara.calendar_db_generator.work_event_generator import WorkEventGenerator
from tara.calendar_db_generator.personal_event_generator import PersonalEventGenerator
from tara.calendar_db_generator.attendees_generator import AttendeesGenerator
from tara.calendar_db_generator.sql_generator import SQLGenerator

class CalendarDatabasePipeline(Pipeline):
    def __init__(self, checkpoint_dir: str = "output/checkpoints"):
        super().__init__()
        self.profiles = None
        self.users_df = None
        self.calendars_df = None
        self.events_df = None
        self.attendees_df = None
        self.checkpoint_dir = checkpoint_dir
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Create checkpoint directory
        if not os.path.exists(self.checkpoint_dir):
            os.makedirs(self.checkpoint_dir)
        
        # Get project root directory - simple approach
        self.project_root = os.getcwd()
        
        # Load all data files
        self.company_organization = self._load_data('seed_data/company_organization.json')
        self.work_event_templates = self._load_data('seed_data/work_event_template.json')
        
    def _save_checkpoint(self, step_name: str, data: Any):
        """Save checkpoint data"""
        checkpoint_file = os.path.join(self.checkpoint_dir, f"{step_name}.pkl")
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(data, f)
        self.logger.info(f"Saved checkpoint: {step_name}")
    
    def _load_checkpoint(self, step_name: str) -> Optional[Any]:
        """Load checkpoint data"""
        checkpoint_file = os.path.join(self.checkpoint_dir, f"{step_name}.pkl")
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'rb') as f:
                data = pickle.load(f)
            self.logger.info(f"Loaded checkpoint: {step_name}")
            return data
        return None
    
    def _checkpoint_exists(self, step_name: str) -> bool:
        """Check if checkpoint exists"""
        checkpoint_file = os.path.join(self.checkpoint_dir, f"{step_name}.pkl")
        return os.path.exists(checkpoint_file)
    
    def _load_data(self, file_path: str) -> Any:
        """Method to load data from JSON files"""
        full_path = os.path.join(self.project_root, file_path)
        
        try:
            with open(full_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading data from {full_path}: {e}")
            return None
        
    def generate_profiles(self, company_organization: List[Dict[str, Any]], skip_if_exists: bool = True) -> List[Dict[str, Any]]:
        """Generate user profiles from company organization data"""
        if skip_if_exists and self._checkpoint_exists('profiles'):
            self.logger.info("Loading profiles from checkpoint...")
            return self._load_checkpoint('profiles')
        
        self.logger.info("Generating user profiles...")
        
        # Create DataFrame from company organization data
        self.df = pd.DataFrame(company_organization)
        
        # Initialize profile generator
        profile_generator = ProfileGenerator(model=self.model)
        
        # Execute action for generating user profiles
        self.execute_action(profile_generator.generate_user_profile, 'generated_profile', parallel=True)
        
        # Extract successful profiles
        profiles = []
        for _, row in self.df.iterrows():
            if row['generated_profile'] is not None:
                profiles.append(row['generated_profile'])
            else:
                self.logger.warning(f"Failed to generate profile for {row['first_name']} {row['last_name']}")
        
        self.logger.info(f"Generated {len(profiles)} user profiles")
        
        # Save checkpoint
        self._save_checkpoint('profiles', profiles)
        
        return profiles
    
    def create_users_dataframe(self, profiles: List[Dict[str, Any]], skip_if_exists: bool = True) -> pd.DataFrame:
        """Create users DataFrame from profiles - SQLite will auto-generate IDs"""
        if skip_if_exists and self._checkpoint_exists('users_df'):
            self.logger.info("Loading users DataFrame from checkpoint...")
            df = self._load_checkpoint('users_df')
            df.to_csv("output/users.csv", index=False)
            self.logger.info(f"Saved users.csv with {len(df)} records")
            return df
        
        self.logger.info("Creating users DataFrame...")
        users_data = []
        
        for profile in profiles:
            user_record = {
                'email': profile['personal']['email'],
                'name': profile['personal']['name'],
                'created_at': profile['professional']['start_date'],
                'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                'session_id': None  # Populated by SQL generator with @session_id variable
            }
            users_data.append(user_record)
        
        df = pd.DataFrame(users_data)
        df.to_csv("output/users.csv", index=False)
        self.logger.info(f"Saved users.csv with {len(df)} records")
        
        self._save_checkpoint('users_df', df)
        return df
    
    def create_calendars_dataframe(self, profiles: List[Dict[str, Any]], skip_if_exists: bool = True) -> pd.DataFrame:
        """Create calendars DataFrame with TEXT primary keys (user_X_personal, user_X_work)"""
        if skip_if_exists and self._checkpoint_exists('calendars_df'):
            self.logger.info("Loading calendars DataFrame from checkpoint...")
            df = self._load_checkpoint('calendars_df')
            df.to_csv("output/calendars.csv", index=False)
            self.logger.info(f"Saved calendars.csv with {len(df)} records")
            return df
        
        self.logger.info("Creating calendars DataFrame...")
        calendars_data = []
        
        # Each user gets two calendars: Personal and Work
        calendar_templates = [
            {'name': 'Personal', 'color': '#34a853', 'text_color': '#ffffff'},
            {'name': 'Work', 'color': '#4285f4', 'text_color': '#ffffff'}
        ]
        
        for user_index, profile in enumerate(profiles):
            user_id = user_index + 1
            
            for template in calendar_templates:
                # Generate TEXT calendar ID: user_X_personal or user_X_work
                calendar_id = f"user_{user_id}_{template['name'].lower()}"
                
                calendar_record = {
                    'id': calendar_id,  # TEXT primary key
                    'name': template['name'],
                    'color': template['color'],
                    'text_color': template['text_color'],
                    'owner_id': user_id,
                    'created_at': profile['professional']['start_date'],
                    'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'session_id': None  # Populated by SQL generator with @session_id variable
                }
                calendars_data.append(calendar_record)
        
        df = pd.DataFrame(calendars_data)
        df.to_csv("output/calendars.csv", index=False)
        self.logger.info(f"Saved calendars.csv with {len(df)} records")
        
        self._save_checkpoint('calendars_df', df)
        return df
    
    def _get_user_calendar(self, user_id: int, calendar_name: str, calendars: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Get a specific calendar for a user"""
        for calendar in calendars:
            if calendar['owner_id'] == user_id and calendar['name'] == calendar_name:
                return calendar
        return None


    
    def generate_events(self, profiles: List[Dict[str, Any]], calendars_df: pd.DataFrame, skip_if_exists: bool = True) -> pd.DataFrame:
        """Generate work and personal events for all users - SQLite will auto-generate IDs"""
        if skip_if_exists and self._checkpoint_exists('events_df'):
            self.logger.info("Loading events DataFrame from checkpoint...")
            df = self._load_checkpoint('events_df')
            df.to_csv("output/events.csv", index=False)
            self.logger.info(f"Saved events.csv with {len(df)} records")
            return df
        
        self.logger.info("Generating events...")
        calendars = calendars_df.to_dict('records')
        
        # Generate work events for each user
        work_data = []
        for user_index, profile in enumerate(profiles):
            user_id = user_index + 1
            work_calendar = self._get_user_calendar(user_id, 'Work', calendars)
            if work_calendar:
                work_data.append({
                    'user_id': user_id,
                    'work_calendar_id': work_calendar['id'],
                    'work_event_templates': self.work_event_templates
                })
        
        self.df = pd.DataFrame(work_data)
        work_generator = WorkEventGenerator(model=self.model)
        self.execute_action(work_generator.generate_work_events_for_user, 'work_events', parallel=True)
        
        # Extract work events
        work_events = []
        for _, row in self.df.iterrows():
            if row['work_events'] is not None:
                work_events.extend(row['work_events'])
        
        # Generate personal events for each user
        personal_data = []
        for user_index, profile in enumerate(profiles):
            user_id = user_index + 1
            personal_calendar = self._get_user_calendar(user_id, 'Personal', calendars)
            if personal_calendar:
                # Pass user's work events to personal generator for context
                user_work_events = [e for e in work_events if e['user_id'] == user_id]
                personal_data.append({
                    'user_id': user_id,
                    'profile': profile,
                    'personal_calendar_id': personal_calendar['id'],
                    'work_events': user_work_events
                })
        
        self.df = pd.DataFrame(personal_data)
        personal_generator = PersonalEventGenerator(model=self.model)
        self.execute_action(personal_generator.generate_personal_events_for_user, 'personal_events', parallel=True)
        
        # Extract personal events
        personal_events = []
        for _, row in self.df.iterrows():
            if row['personal_events'] is not None:
                personal_events.extend(row['personal_events'])
        
        # Combine all events
        all_events = work_events + personal_events
        self.logger.info(f"Generated {len(work_events)} work events and {len(personal_events)} personal events")
        
        df = pd.DataFrame(all_events)
        df.to_csv("output/events.csv", index=False)
        self.logger.info(f"Saved events.csv with {len(df)} records")
        
        self._save_checkpoint('events_df', df)
        return df
    
    def generate_attendees(self, events_df: pd.DataFrame, profiles: List[Dict[str, Any]], skip_if_exists: bool = True) -> pd.DataFrame:
        """Generate attendees - one attendee record per unique event title per user"""
        if skip_if_exists and self._checkpoint_exists('attendees_df'):
            self.logger.info("Loading attendees DataFrame from checkpoint...")
            df = self._load_checkpoint('attendees_df')
            df.to_csv("output/attendees.csv", index=False)
            self.logger.info(f"Saved attendees.csv with {len(df)} records")
            return df
        
        self.logger.info("Generating attendees...")
        
        # Get unique events (deduplicate by user_id + title to avoid duplicate attendees)
        unique_events = []
        seen_events = set()
        
        for event in events_df.to_dict('records'):
            key = (event['user_id'], event['title'])
            if key not in seen_events:
                seen_events.add(key)
                unique_events.append(event)
        
        self.logger.info(f"Processing {len(unique_events)} unique events (deduplicated from {len(events_df)} total events)")
        
        # Generate attendees for unique events only
        attendees_generator = AttendeesGenerator(model=self.model)
        attendee_data = []
        
        for event in unique_events:
            attendee_data.append({
                'event': event,
                'profiles': profiles
            })
        
        self.df = pd.DataFrame(attendee_data)
        self.execute_action(attendees_generator.generate_attendees_for_event, 'attendees', parallel=True)
        
        # Extract attendees
        attendees = []
        for _, row in self.df.iterrows():
            if row['attendees'] is not None:
                attendees.extend(row['attendees'])
        
        self.logger.info(f"Generated {len(attendees)} attendee records")
        
        df = pd.DataFrame(attendees)
        df.to_csv("output/attendees.csv", index=False)
        self.logger.info(f"Saved attendees.csv with {len(df)} records")
        
        self._save_checkpoint('attendees_df', df)
        return df
    
    def generate_sql_file(self, output_file: str = "calendar_data_final.sql"):
        """Generate SQL file with schema and INSERT statements using @TODAY and @session_id variables"""
        self.logger.info("Generating SQL file...")
        
        generator = SQLGenerator()
        generator.generate_sql(output_file)
        
        self.logger.info(f"SQL file generated: {output_file}")
    
    def execute_sql_file(self, sql_file: str = "calendar_data_final.sql", db_file: str = "calendar.db", today: str = None, session_id: str = None):
        """Execute SQL file with variable replacement on SQLite database"""
        self.logger.info(f"Executing SQL file on SQLite database: {db_file}")
        
        generator = SQLGenerator()
        generator.execute_sql_file(sql_file, db_file, today, session_id)
        
        self.logger.info(f"SQL file executed successfully on {db_file}")
    
    def process(self, skip_steps: List[str] = None, force_steps: List[str] = None):
        """Main pipeline process with checkpoint support"""
        self.logger.info("Starting Calendar Database Pipeline")
        self.logger.info("=" * 50)
        
        if skip_steps:
            self.logger.info(f"Skipping steps: {', '.join(skip_steps)}")
        if force_steps:
            self.logger.info(f"Forcing steps: {', '.join(force_steps)}")
        
        # Step 1: Load company organization
        company_organization = self.company_organization
        if not company_organization:
            self.logger.error("Failed to load company organization")
            return
        
        # Step 2: Generate profiles
        if 'profiles' not in (skip_steps or []):
            skip_profiles = 'profiles' not in (force_steps or [])
            self.profiles = self.generate_profiles(company_organization, skip_if_exists=skip_profiles)
        else:
            self.logger.info("Skipping profile generation")
            # Load profiles from checkpoint when skipping
            self.profiles = self._load_checkpoint('profiles') or []
        
        # Step 3: Create DataFrames
        if 'dataframes' not in (skip_steps or []):
            skip_users = 'dataframes' not in (force_steps or [])
            skip_calendars = 'dataframes' not in (force_steps or [])
            
            self.users_df = self.create_users_dataframe(self.profiles, skip_if_exists=skip_users)
            self.calendars_df = self.create_calendars_dataframe(self.profiles, skip_if_exists=skip_calendars)
        else:
            self.logger.info("Skipping DataFrame creation")
            # Load DataFrames from checkpoints when skipping
            self.users_df = self._load_checkpoint('users_df')
            self.calendars_df = self._load_checkpoint('calendars_df')
        
        # Step 4: Generate events
        if 'events' not in (skip_steps or []):
            skip_events = 'events' not in (force_steps or [])
            self.events_df = self.generate_events(self.profiles, self.calendars_df, skip_if_exists=skip_events)
        else:
            self.logger.info("Skipping event generation")
            # Load events from checkpoint when skipping
            self.events_df = self._load_checkpoint('events_df')
        
        # Step 5: Generate attendees
        if 'attendees' not in (skip_steps or []):
            skip_attendees = 'attendees' not in (force_steps or [])
            self.attendees_df = self.generate_attendees(self.events_df, self.profiles, skip_if_exists=skip_attendees)
        else:
            self.logger.info("Skipping attendee generation")
            # Load attendees from checkpoint when skipping
            self.attendees_df = self._load_checkpoint('attendees_df')
        
        # Step 6: Generate SQL file
        if 'sql' not in (skip_steps or []):
            self.generate_sql_file()
        else:
            self.logger.info("Skipping SQL file generation")
        
        # Step 7: Execute SQL file (optional)
        if 'execute_sql' not in (skip_steps or []):
            self.execute_sql_file()
        else:
            self.logger.info("Skipping SQL execution")
        
        self.logger.info("Pipeline completed successfully!")
        self.logger.info("Generated data:")
        if self.users_df is not None:
            self.logger.info(f"  - Users: {len(self.users_df)}")
        if self.calendars_df is not None:
            self.logger.info(f"  - Calendars: {len(self.calendars_df)}")
        if self.events_df is not None:
            self.logger.info(f"  - Events: {len(self.events_df)}")
        if self.attendees_df is not None:
            self.logger.info(f"  - Attendees: {len(self.attendees_df)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Calendar Database Pipeline')
    parser.add_argument('--model', default='openai/o4-mini', help='AI model to use')
    parser.add_argument('--skip', nargs='*', help='Steps to skip (profiles, dataframes, events, attendees, sql, execute_sql)')
    parser.add_argument('--force', nargs='*', help='Steps to force re-run (profiles, dataframes, events, attendees, sql, execute_sql)')
    parser.add_argument('--checkpoint-dir', default='output/checkpoints', help='Checkpoint directory')
    
    args = parser.parse_args()
    
    pipeline = CalendarDatabasePipeline(checkpoint_dir=args.checkpoint_dir)
    pipeline.model = args.model
    
    skip_steps = args.skip or []
    force_steps = args.force or []
    
    pipeline.process(skip_steps=skip_steps, force_steps=force_steps)