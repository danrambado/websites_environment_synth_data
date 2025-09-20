from typing import List, Dict, Any
from tara.lib.action import Action


class WorkEventGenerator(Action):
    """Action to generate work events for a single user"""
    
    def __init__(self, model=None):
        super().__init__(model)

    def generate_work_events_for_user(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate work events for a single user
        """
        user_id = row['user_id']
        calendar_id = row['work_calendar_id']
        
        # Get work event templates from pipeline
        work_event_templates = row['work_event_templates']
        
        events = []
        
        # Generate events based on templates
        for template in work_event_templates:
            if template['type'] == 'daily':
                new_events = self._generate_daily_events(user_id, calendar_id, template)
            elif template['type'] == 'weekly':
                new_events = self._generate_weekly_events(user_id, calendar_id, template)
            elif template['type'] == 'biweekly':
                new_events = self._generate_biweekly_events(user_id, calendar_id, template)
            elif template['type'] == 'monthly':
                new_events = self._generate_monthly_events(user_id, calendar_id, template)
            
            events.extend(new_events)
        
        return events

    def _generate_daily_events(self, user_id: int, calendar_id: int, template: Dict) -> List[Dict[str, Any]]:
        """Generate daily recurring events"""
        events = []
        from datetime import datetime, timedelta, time
        
        start_date = datetime.now() - timedelta(days=30)  # 1 month back
        end_date = datetime.now() + timedelta(days=180)   # 6 months ahead
        
        current_date = start_date
        
        while current_date <= end_date:
            # Skip weekends for work events
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                start_time = datetime.combine(current_date, time.fromisoformat(template['time']))
                end_time = start_time + timedelta(minutes=template['duration'])
                
                event_record = {
                    'user_id': user_id,
                    'title': template['name'],
                    'description': template['description'],
                    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'location': 'Conference Room A',
                    'all_day': False,
                    'calendar_id': calendar_id,
                    'rrule': 'RRULE:FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR',
                    'duration': None,
                    'exdate': None,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'session_id': None  # Populated by SQL generator with @session_id variable
                }
                events.append(event_record)
            
            current_date += timedelta(days=1)
        
        return events

    def _generate_weekly_events(self, user_id: int, calendar_id: int, template: Dict) -> List[Dict[str, Any]]:
        """Generate weekly recurring events"""
        events = []
        from datetime import datetime, timedelta, time
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now() + timedelta(days=180)
        
        day_map = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
        target_day = day_map.get(template['day'], 0)
        
        current_date = start_date
        
        while current_date <= end_date:
            # Find the next occurrence of the target day
            days_ahead = target_day - current_date.weekday()
            if days_ahead <= 0:  # Target day already happened this week
                days_ahead += 7
            target_date = current_date + timedelta(days=days_ahead)
            
            if target_date <= end_date:
                start_time = datetime.combine(target_date, time.fromisoformat(template['time']))
                end_time = start_time + timedelta(minutes=template['duration'])
                
                event_record = {
                    'user_id': user_id,
                    'title': template['name'],
                    'description': template['description'],
                    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'location': 'Conference Room B',
                    'all_day': False,
                    'calendar_id': calendar_id,
                    'rrule': f'RRULE:FREQ=WEEKLY;BYDAY={template["day"][:2].upper()}',
                    'duration': None,
                    'exdate': None,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'session_id': None  # Populated by SQL generator with @session_id variable
                }
                events.append(event_record)
            
            current_date += timedelta(days=7)
        
        return events

    def _generate_biweekly_events(self, user_id: int, calendar_id: int, template: Dict) -> List[Dict[str, Any]]:
        """Generate biweekly recurring events"""
        events = []
        from datetime import datetime, timedelta, time
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now() + timedelta(days=180)
        
        day_map = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
        target_day = day_map.get(template['day'], 0)
        
        current_date = start_date
        
        while current_date <= end_date:
            days_ahead = target_day - current_date.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            target_date = current_date + timedelta(days=days_ahead)
            
            if target_date <= end_date:
                start_time = datetime.combine(target_date, time.fromisoformat(template['time']))
                end_time = start_time + timedelta(minutes=template['duration'])
                
                event_record = {
                    'user_id': user_id,
                    'title': template['name'],
                    'description': template['description'],
                    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'location': 'Conference Room C',
                    'all_day': False,
                    'calendar_id': calendar_id,
                    'rrule': f'RRULE:FREQ=WEEKLY;INTERVAL=2;BYDAY={template["day"][:2].upper()}',
                    'duration': None,
                    'exdate': None,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'session_id': None,  # Will be populated by SQL generator
                }
                events.append(event_record)
            
            current_date += timedelta(days=14)
        
        return events

    def _generate_monthly_events(self, user_id: int, calendar_id: int, template: Dict) -> List[Dict[str, Any]]:
        """Generate monthly recurring events"""
        events = []
        from datetime import datetime, timedelta, time
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now() + timedelta(days=180)
        
        current_date = start_date
        
        while current_date <= end_date:
            # First Friday of the month
            first_friday = current_date.replace(day=1)
            while first_friday.weekday() != 4:  # Friday = 4
                first_friday += timedelta(days=1)
            
            if first_friday >= start_date and first_friday <= end_date:
                start_time = datetime.combine(first_friday, time.fromisoformat(template['time']))
                end_time = start_time + timedelta(minutes=template['duration'])
                
                event_record = {
                    'user_id': user_id,
                    'title': template['name'],
                    'description': template['description'],
                    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'location': 'Main Conference Room',
                    'all_day': False,
                    'calendar_id': calendar_id,
                    'rrule': 'RRULE:FREQ=MONTHLY;BYDAY=1FR',
                    'duration': None,
                    'exdate': None,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'session_id': None,  # Will be populated by SQL generator
                }
                events.append(event_record)
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        return events
