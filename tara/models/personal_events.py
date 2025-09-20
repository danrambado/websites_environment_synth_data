"""
Personal Events Data Models
Pydantic models for personal event generation
"""

from pydantic import BaseModel, Field
from typing import List


class PersonalEventData(BaseModel):
    """Personal event data structure"""
    title: str = Field(description="Event title")
    description: str = Field(description="Event description")
    start_time: str = Field(description="Event start time in ISO format")
    end_time: str = Field(description="Event end time in ISO format")
    location: str = Field(description="Event location")
    is_all_day: bool = Field(description="Whether the event is all day")
    recurrence_rule: str = Field(default="", description="Recurrence rule if applicable")


class PersonalEventsResponse(BaseModel):
    """Response structure for personal events generation"""
    personal_events: List[PersonalEventData] = Field(description="List of personal events")


def get_personal_events_schema():
    """Get the JSON schema for personal events generation"""
    return PersonalEventsResponse.model_json_schema()
