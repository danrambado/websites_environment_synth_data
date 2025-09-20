import json
import random
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from tara.lib.action import Action
from tara.models import Attendee


class AttendeesGenerator(Action):
    """Action to generate attendees for events"""
    
    def __init__(self, model=None):
        super().__init__(model)
        
        # RSVP status probabilities
        self.rsvp_probabilities = {
            'accepted': 0.75,    # 75% accept
            'declined': 0.10,    # 10% decline
            'tentative': 0.10,   # 10% tentative
            'no_response': 0.05  # 5% no response
        }
        
        # Meeting roles
        self.meeting_roles = [
            'organizer',
            'required_attendee', 
            'optional_attendee',
            'resource'
        ]

    def generate_attendees_for_event(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate attendees for a specific event
        """
        event = row['event']
        profiles = row['profiles']
        
        attendees = []
        organizer_id = event['user_id']
        
        # Determine attendee list based on event type
        attendee_ids = self._get_event_attendees(event, organizer_id, profiles)
        
        # Generate attendee records
        for i, attendee_id in enumerate(attendee_ids):
            # Determine role
            role = self._determine_attendee_role(attendee_id, organizer_id, i, len(attendee_ids))
            
            # Determine RSVP status
            rsvp_status = self._determine_rsvp_status(role, event, attendee_id)
            
            # Generate response timestamp
            response_timestamp = self._generate_response_timestamp(event, rsvp_status)
            
            # Generate attendee record
            attendee_record = {
                'user_id': attendee_id,
                'event_user_id': organizer_id,  # The user who created the event
                'event_title': event['title'],  # Event title for matching in SQL
                'event_start_time': event['start_time'],  # Event start time for matching in SQL
                'event_end_time': event['end_time'],  # Event end time for matching in SQL
                'status': rsvp_status,
                'responded_at': response_timestamp,
                'session_id': None  # Populated by SQL generator with @session_id variable
            }
            
            attendees.append(attendee_record)
        
        return attendees

    def _get_event_attendees(self, event: Dict[str, Any], organizer_id: int, profiles: List[Dict[str, Any]]) -> List[int]:
        """Determine who should attend based on event type and organizer"""
        event_title = event['title']
        calendar_id = event['calendar_id']
        
        # Determine calendar type based on calendar ID pattern
        # Calendar IDs follow pattern: Personal (1,4,7...), Work (2,5,8...), Holiday (3,6,9...)
        calendar_type = self._determine_calendar_type(calendar_id)
        
        if calendar_type == 'Personal':
            # Personal events - usually just the organizer, sometimes family
            if any(keyword in event_title.lower() for keyword in ['family', 'birthday', 'anniversary', 'wedding']):
                # Family events - add family members (simplified)
                return [organizer_id] + self._get_family_members(organizer_id, profiles)
            else:
                # Personal events - just organizer
                return [organizer_id]
        
        elif calendar_type == 'Work':
            # Work events - determine attendees based on event type
            if '1:1 with Manager' in event_title:
                # 1:1 with manager - organizer + manager
                manager_id = self._get_manager_id(organizer_id, profiles)
                return [organizer_id, manager_id] if manager_id else [organizer_id]
            
            elif 'Daily Standup' in event_title:
                # Daily standup - team members
                return self._get_team_members(organizer_id, profiles)
            
            elif any(keyword in event_title.lower() for keyword in ['sprint', 'planning', 'review', 'retrospective']):
                # Sprint events - team members
                return self._get_team_members(organizer_id, profiles)
            
            elif 'All Hands' in event_title:
                # All hands - all company members
                return [i + 1 for i in range(len(profiles))]  # Convert index to user_id
            
            elif 'Team Meeting' in event_title:
                # Team meeting - team members
                return self._get_team_members(organizer_id, profiles)
            
            elif 'Code Review' in event_title:
                # Code review - team members + reviewers
                team_members = self._get_team_members(organizer_id, profiles)
                reviewers = self._get_code_reviewers(organizer_id, profiles)
                return list(set(team_members + reviewers))
            
            else:
                # Default work event - team members
                return self._get_team_members(organizer_id, profiles)
        
        else:
            # Unknown calendar type - just organizer
            return [organizer_id]

    def _determine_calendar_type(self, calendar_id: str) -> str:
        """Determine calendar type based on calendar ID pattern (user_X_personal or user_X_work)"""
        if calendar_id.endswith('_personal'):
            return 'Personal'
        elif calendar_id.endswith('_work'):
            return 'Work'
        else:
            # Fallback for unexpected calendar ID format
            return 'Work'

    def _get_manager_id(self, user_id: int, profiles: List[Dict[str, Any]]) -> Optional[int]:
        """Get manager ID for a user"""
        try:
            # Profiles are indexed by position, user_id corresponds to index + 1
            user_profile = profiles[user_id - 1] if user_id <= len(profiles) else None
            if user_profile and 'professional' in user_profile:
                manager_name = user_profile['professional'].get('reports_to')
                if manager_name and manager_name != '-' and manager_name != '':
                    # Find the manager's user ID by name
                    for i, profile in enumerate(profiles):
                        if profile.get('professional', {}).get('name') == manager_name:
                            return i + 1  # Convert index to user_id
        except (ValueError, TypeError, KeyError, IndexError):
            pass
        return None

    def _get_team_members(self, user_id: int, profiles: List[Dict[str, Any]]) -> List[int]:
        """Get team members for a user"""
        try:
            # Profiles are indexed by position, user_id corresponds to index + 1
            user_profile = profiles[user_id - 1] if user_id <= len(profiles) else None
            if user_profile and 'professional' in user_profile:
                team_name = user_profile['professional'].get('team', '')
                # Find all users in the same team
                team_members = []
                for i, profile in enumerate(profiles):
                    if profile.get('professional', {}).get('team') == team_name:
                        team_members.append(i + 1)  # Convert index to user_id
                return team_members if team_members else [user_id]
        except (KeyError, TypeError, IndexError):
            pass
        return [user_id]

    def _get_family_members(self, user_id: int, profiles: List[Dict[str, Any]]) -> List[int]:
        """Get family members for personal events (simplified)"""
        # For now, just return empty list
        # In a real implementation, this would look up family relationships
        return []

    def _get_code_reviewers(self, user_id: int, profiles: List[Dict[str, Any]]) -> List[int]:
        """Get code reviewers for code review events"""
        # For now, return team members
        return self._get_team_members(user_id, profiles)

    def _determine_attendee_role(self, attendee_id: int, organizer_id: int, position: int, total_attendees: int) -> str:
        """Determine the role of an attendee"""
        if attendee_id == organizer_id:
            return 'organizer'
        elif position == 1 and total_attendees > 1:  # Second person is often required
            return 'required_attendee'
        else:
            return random.choices(
                ['required_attendee', 'optional_attendee'],
                weights=[0.6, 0.4]
            )[0]

    def _determine_rsvp_status(self, role: str, event: Dict[str, Any], attendee_id: int) -> str:
        """Determine RSVP status based on role, event type, and attendee"""
        # Organizers always accept
        if role == 'organizer':
            return 'accepted'
        
        # Required attendees have higher acceptance rate
        if role == 'required_attendee':
            return random.choices(
                list(self.rsvp_probabilities.keys()),
                weights=[0.85, 0.05, 0.08, 0.02]  # Higher acceptance for required
            )[0]
        
        # Optional attendees have lower acceptance rate
        elif role == 'optional_attendee':
            return random.choices(
                list(self.rsvp_probabilities.keys()),
                weights=[0.60, 0.20, 0.15, 0.05]  # Lower acceptance for optional
            )[0]
        
        # Default probabilities
        return random.choices(
            list(self.rsvp_probabilities.keys()),
            weights=list(self.rsvp_probabilities.values())
        )[0]

    def _generate_response_timestamp(self, event: Dict[str, Any], rsvp_status: str) -> Optional[str]:
        """Generate realistic response timestamp based on RSVP status - returns dynamic SQL expression"""
        if rsvp_status == 'no_response':
            return None
        
        # Response times vary based on status (days before event)
        if rsvp_status == 'accepted':
            # Accepted responses usually come quickly
            response_days_before = random.randint(1, 7)
        elif rsvp_status == 'declined':
            # Declined responses come quickly too
            response_days_before = random.randint(1, 5)
        else:  # tentative
            # Tentative responses take longer
            response_days_before = random.randint(3, 14)
        
        # Return dynamic SQL expression that calculates response time relative to @TODAY
        # Format: DATE_ADD(@TODAY, INTERVAL days_offset DAY) + INTERVAL '10:00:00' HOUR_SECOND
        response_time = f"DATE_ADD(@TODAY, INTERVAL {response_days_before} DAY) + INTERVAL '10:00:00' HOUR_SECOND"
        return response_time

    def _generate_rsvp_note(self, rsvp_status: str) -> Optional[str]:
        """Generate realistic RSVP notes"""
        if rsvp_status == 'accepted':
            return random.choice([
                "Looking forward to it!",
                "Will be there",
                "Confirmed",
                None
            ])
        elif rsvp_status == 'declined':
            return random.choice([
                "Sorry, have a conflict",
                "Can't make it this time",
                "Out of office",
                None
            ])
        elif rsvp_status == 'tentative':
            return random.choice([
                "Maybe, will confirm later",
                "Tentative - depends on other meetings",
                "Will try to make it",
                None
            ])
        else:
            return None