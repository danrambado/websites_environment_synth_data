"""
Tara Models Package
Contains all Pydantic data models for the calendar database generator
"""

from .user_profile import (
    Location,
    Personal,
    WorkPreferences,
    ProfessionalHierarchy,
    Professional,
    Family,
    Hobby,
    MedicalAppointment,
    EmergencyContact,
    Health,
    Education,
    Social,
    Travel,
    PersonalLife,
    UserProfile,
    get_user_profile_schema
)

from .calendar import (
    Calendar,
    Event,
    Attendee,
    User,
    WorkEventTemplate,
    PersonalEventTemplate,
    HolidayEvent
)

from .personal_events import (
    PersonalEventData,
    PersonalEventsResponse,
    get_personal_events_schema
)

__all__ = [
    # User Profile Models
    'Location',
    'Personal',
    'WorkPreferences',
    'ProfessionalHierarchy',
    'Professional',
    'Family',
    'Hobby',
    'MedicalAppointment',
    'EmergencyContact',
    'Health',
    'Education',
    'Social',
    'Travel',
    'PersonalLife',
    'UserProfile',
    'get_user_profile_schema',
    
    # Calendar Models
    'Calendar',
    'Event',
    'Attendee',
    'User',
    'WorkEventTemplate',
    'PersonalEventTemplate',
    'HolidayEvent',
    
    # Personal Event Models
    'PersonalEventData',
    'PersonalEventsResponse',
    'get_personal_events_schema'
]
