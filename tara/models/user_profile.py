"""
User Profile Data Models
Pydantic models for user profile generation
"""

from pydantic import BaseModel, Field
from typing import List, Optional


class Location(BaseModel):
    """Location information for a user"""
    city: str
    state: str
    timezone: str
    country: str = "USA"


class Personal(BaseModel):
    """Personal information for a user"""
    name: str
    email: str
    age: int = Field(ge=18, le=70)
    pronouns: str
    birthday: str
    phone: Optional[str] = None
    location: Location
    languages: List[str]
    personality_traits: List[str]


class WorkPreferences(BaseModel):
    """Work preferences and schedule preferences"""
    remote_days: List[str]
    preferred_meeting_times: List[str]
    focus_time_blocks: List[str]
    communication_style: str
    timezone_preference: str


class ProfessionalHierarchy(BaseModel):
    """Professional relationships and hierarchy"""
    manager_id: Optional[str] = None
    direct_reports: List[str] = []
    peers: List[str] = []
    collaborators: List[str] = []
    mentees: List[str] = []
    mentors: List[str] = []


class Professional(BaseModel):
    """Professional information and work details"""
    role: str
    level: str
    department: str
    team: str
    start_date: str
    salary_range: str
    skills: List[str]
    certifications: List[str]
    work_preferences: WorkPreferences
    career_goals: List[str]
    workload: str
    professional_hierarchy: ProfessionalHierarchy


class Family(BaseModel):
    """Family information"""
    marital_status: str
    children: List[str] = []
    pets: List[str] = []


class Hobby(BaseModel):
    """Hobby information with scheduling preferences"""
    name: str
    frequency: str
    day_preference: str
    time_preference: str


class MedicalAppointment(BaseModel):
    """Medical appointment information"""
    type: str
    frequency: str
    preferred_day: str
    preferred_time: str


class EmergencyContact(BaseModel):
    """Emergency contact information"""
    name: str
    relationship: str
    phone: str


class Health(BaseModel):
    """Health and wellness information"""
    gym_member: bool
    gym_schedule: List[str]
    doctor_appointments: str
    medical_appointments: List[MedicalAppointment]
    health_conditions: List[str]
    medications: List[str]
    emergency_contact: EmergencyContact


class Education(BaseModel):
    """Education and learning information"""
    current_studies: str
    learning_schedule: List[str]
    time_preference: str
    degree_level: str
    field_of_study: str
    learning_style: str


class Social(BaseModel):
    """Social and community involvement"""
    social_media_usage: str
    networking_events: str
    volunteer_work: str
    community_involvement: List[str]


class Travel(BaseModel):
    """Travel preferences and habits"""
    travel_frequency: str
    travel_preferences: List[str]
    vacation_time: str


class PersonalLife(BaseModel):
    """Personal life information combining all personal aspects"""
    family: Family
    hobbies: List[Hobby]
    health: Health
    education: Education
    social: Social
    travel: Travel


class UserProfile(BaseModel):
    """Complete user profile combining personal, professional, and personal life"""
    personal: Personal
    professional: Professional
    personal_life: PersonalLife


def get_user_profile_schema():
    """Get the JSON schema for user profile generation"""
    return UserProfile.model_json_schema()
