import json
import os
from typing import List, Dict, Any
from tara.lib.action import Action
from tara.models import PersonalEventsResponse, get_personal_events_schema
from datetime import datetime, timedelta, time


class PersonalEventGenerator(Action):
    """Action to generate personal events for a single user using AI"""
    
    def __init__(self, model=None):
        super().__init__(model)

    def generate_personal_events_for_user(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate personal events for a specific user using AI
        """
        user_id = row['user_id']
        profile = row['profile']
        calendar_id = row['personal_calendar_id']
        work_events = row['work_events']
        
        # Debug: Check profile structure
        if profile and 'personal_life' not in profile:
            print(f"🔍 Debug - Missing personal_life key for user {user_id}. Available keys: {list(profile.keys())}")
            # Return empty list for users with incomplete profiles
            return []
        
        # Check if personal_life has the expected structure
        if profile and 'personal_life' in profile:
            personal_life = profile['personal_life']
            if 'family' not in personal_life:
                print(f"🔍 Debug - personal_life missing 'family' key. Available: {list(personal_life.keys())}")
                return []
        
        # Create work schedule summary internally
        work_schedule_summary = self._create_work_schedule_summary(work_events)
        
        prompt = f"""
You are an expert at creating realistic personal calendar events for software company employees.
Generate a comprehensive set of personal events for the next 6 months based on this person's profile and existing work schedule.

User Profile:
- Name: {profile['personal']['name']}
- Age: {profile['personal']['age']}
- Role: {profile['professional']['role']}
- Team: {profile['professional']['team']}
- Work Style: {profile['professional']['work_preferences']['communication_style']}
- Family Status: {profile['personal_life']['family']['marital_status']}
- Hobbies: {', '.join([h['name'] for h in profile['personal_life']['hobbies']])}
- Health: {profile['personal_life']['health']['gym_member']}
- Travel: {profile['personal_life']['travel']['travel_frequency']}

Existing Work Schedule Summary:
{work_schedule_summary}

Generate realistic personal events that would fit this person's lifestyle and work schedule. Consider:
1. Doctor appointments, dentist visits, health checkups
2. Personal errands and shopping
3. Social events, dinners, parties
4. Vacation days and personal time off
5. Family events, birthdays, anniversaries
6. Fitness activities, gym sessions, sports
7. Personal projects and hobbies
8. Travel and weekend trips
9. Home maintenance and chores
10. Personal development activities

Requirements:
- Events should be realistic and fit their lifestyle
- Avoid conflicts with existing work schedule
- Include appropriate durations (15min to 4 hours)
- Mix recurring and one-time events
- Consider their role level and work-life balance
- Include some weekend and evening events
- Make events personal and specific to their interests
- IMPORTANT: For recurrence_rule field, use empty string "" for one-time events, or proper RRULE format for recurring events (e.g., "FREQ=WEEKLY;BYDAY=MO" for weekly on Mondays)
- NEVER use null or None for recurrence_rule - always use empty string "" if not recurring

Generate 15-25 personal events that would realistically occur over 6 months.
"""

        # Get schema from Pydantic model
        schema = get_personal_events_schema()

        try:
            response = self.prompt(prompt, schema=schema)
            
            # Parse JSON first to fix any None values
            events_data = json.loads(response)
            
            # Fix any None recurrence_rule values and date formats
            if 'personal_events' in events_data:
                for event in events_data['personal_events']:
                    if event.get('recurrence_rule') is None:
                        event['recurrence_rule'] = ""
                    
                    # Fix date format issues
                    self._fix_date_format(event, 'start_time')
                    self._fix_date_format(event, 'end_time')
            
            # Parse and validate using Pydantic
            events_response = PersonalEventsResponse.model_validate(events_data)
            
            personal_events = []
            
            for event_data in events_response.personal_events:
                event_record = {
                    'user_id': user_id,
                    'title': event_data.title,
                    'description': event_data.description,
                    'start_time': event_data.start_time,
                    'end_time': event_data.end_time,
                    'location': event_data.location,
                    'all_day': event_data.is_all_day,
                    'calendar_id': calendar_id,
                    'rrule': event_data.recurrence_rule,
                    'duration': None,
                    'exdate': None,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'session_id': None,  # Will be populated by SQL generator
                }
                personal_events.append(event_record)
            
            return personal_events
            
        except Exception as e:
            print(f"Error generating personal events for user {user_id}: {e}")
            return []

    def _fix_date_format(self, event: Dict[str, Any], field: str):
        """Fix common date format issues in event data"""
        if field in event and event[field]:
            date_str = event[field]
            try:
                # Try to parse the date to validate it
                datetime.fromisoformat(date_str)
            except ValueError:
                # Fix common issues
                # Issue 1: Missing zero padding in day (2024-02-5T -> 2024-02-05T)
                import re
                fixed_date = re.sub(r'(\d{4}-\d{1,2})-(\d{1,2})T', r'\1-\2T', date_str)
                fixed_date = re.sub(r'(\d{4}-\d{1,2})-(\d{1,2})T', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}T", fixed_date)
                
                # Issue 2: Missing zero padding in month (2024-2-05T -> 2024-02-05T)
                fixed_date = re.sub(r'(\d{4})-(\d{1,2})-(\d{2})T', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3)}T", fixed_date)
                
                # Validate the fixed date
                try:
                    datetime.fromisoformat(fixed_date)
                    event[field] = fixed_date
                except ValueError:
                    # If still invalid, generate a default date
                    event[field] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    def _create_work_schedule_summary(self, work_events: List[Dict[str, Any]]) -> str:
        """Create a summary of work events for AI context"""
        if not work_events:
            return "No work events scheduled"
        
        # Group events by day of week
        weekly_schedule = {}
        for event in work_events:
            start_time = datetime.fromisoformat(event['start_time'])
            day = start_time.strftime('%A')
            if day not in weekly_schedule:
                weekly_schedule[day] = []
            weekly_schedule[day].append({
                'title': event['title'],
                'time': start_time.strftime('%H:%M'),
                'duration': (datetime.fromisoformat(event['end_time']) - start_time).total_seconds() / 60
            })
        
        summary = "Weekly Work Schedule:\n"
        for day, events in weekly_schedule.items():
            summary += f"- {day}: "
            event_summaries = [f"{e['title']} at {e['time']}" for e in events]
            summary += ", ".join(event_summaries) + "\n"
        
        return summary