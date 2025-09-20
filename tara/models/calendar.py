"""
Calendar Data Models
Pydantic models for calendar, events, and attendees
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class Calendar(BaseModel):
    """Calendar information"""
    id: int
    user_id: int
    name: str
    color: str
    type: str  # 'personal', 'work', 'holiday'
    created_at: str


class Event(BaseModel):
    """Event information"""
    id: int
    user_id: int
    title: str
    description: str
    start_time: str
    end_time: str
    location: str
    calendar_id: int
    is_all_day: bool
    recurrence_rule: Optional[str] = None
    created_at: str
    updated_at: str


class Attendee(BaseModel):
    """Attendee information"""
    event_id: int
    user_id: int
    status: str  # 'pending', 'accepted', 'declined', 'maybe'
    responded_at: Optional[str] = None
    note: Optional[str] = None


class User(BaseModel):
    """User information"""
    id: int
    email: str
    name: str
    created_at: str
    updated_at: str
    primary_calendar_id: Optional[int] = None


class WorkEventTemplate(BaseModel):
    """Template for generating work events"""
    title: str
    description: str
    duration_minutes: int
    frequency: str  # 'daily', 'weekly', 'biweekly', 'monthly'
    day_of_week: Optional[str] = None
    time_slot: str
    location: str
    calendar_type: str
    is_recurring: bool = True
    recurrence_rule: Optional[str] = None


class PersonalEventTemplate(BaseModel):
    """Template for generating personal events"""
    title: str
    description: str
    duration_minutes: int
    frequency: str
    day_of_week: Optional[str] = None
    time_slot: str
    location: str
    calendar_type: str
    is_recurring: bool = False
    recurrence_rule: Optional[str] = None


class HolidayEvent(BaseModel):
    """Holiday event information"""
    name: str
    date: str
    is_federal: bool
    description: str
