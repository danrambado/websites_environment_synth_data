import json
from typing import Dict, Any
from tara.lib.action import Action
from tara.models import get_user_profile_schema, UserProfile


class ProfileGenerator(Action):
    def __init__(self, model=None):
        super().__init__(model)

    def generate_user_profile(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a complete user profile based on company organization data
        This method is designed to work with Pipeline's execute_action
        
        Args:
            row: A row from the DataFrame (company person data)
            
        Returns:
            Complete user profile following the UserProfile schema
        """
        
        prompt = f"""
You are an expert at creating realistic user profiles for software company employees. 
Your task is to generate a complete user profile based on the provided company person information.

Company Person Information:
- Name: {row['first_name']} {row['last_name']}
- Email: {row['email']}
- Position: {row['position']}
- Team: {row['team']}
- Reports to: {row['reports_to']}
- Description: {row['description']}
- Work Behavior: {row['work_behavior']}
- Personal Life: {row['personal_life']}

Based on this information, generate a complete user profile that follows this structure:

1. **Personal Information**: Generate realistic personal details including age, pronouns, birthday, phone, location, languages, and personality traits that align with their work behavior and personal life description.
2. **Professional Information**: Map their position to appropriate role, level, department, team, skills, certifications, work preferences, career goals, and workload. Use the work behavior description to inform work preferences and communication style.
3. **Personal Life**: Generate realistic family status, hobbies (based on their personal life description), health information, education background, social preferences, and travel habits.
4. **Professional Hierarchy**: Set up the professional relationships based on their reports_to field and team structure.

Requirements:
- Make all generated data realistic and consistent with their described personality and work style
- Ensure the profile reflects their position level and responsibilities
- Generate appropriate hobbies and personal interests based on their personal life description
- Create realistic health, education, and social preferences
- Use appropriate pronouns and demographic information
- Ensure all dates are realistic (birthday, start_date, etc.)
- Generate realistic skills and certifications for their role
- Create appropriate work preferences based on their work behavior

CRITICAL SCHEMA REQUIREMENTS:
- ALL fields must be present and have correct data types
- For education.time_preference: use string like "Evening", "Morning", "Weekend"
- For education.learning_style: use string like "Visual", "Auditory", "Kinesthetic", "Reading/Writing"
- For education.current_studies: use string (can be empty "" if not studying)
- For professional_hierarchy.direct_reports: use array of strings (can be empty array [] if no reports)
- NEVER use null/None values - use empty strings "" or empty arrays [] instead
- ALL string fields must be strings, ALL array fields must be arrays

Generate a complete profile that would be realistic for this person in a software company environment.
"""
        # Get schema from models
        schema = get_user_profile_schema()

        # Generate the profile using the model
        response = self.prompt(prompt, schema=schema)
        
        try:
            # Parse the JSON response
            profile_data = json.loads(response)
            
            # Fix common validation issues
            self._fix_profile_validation_issues(profile_data)
            
            # Validate against Pydantic schema to ensure all required fields are present
            validated_profile = UserProfile.model_validate(profile_data)
            return validated_profile.model_dump()
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response for {row['first_name']} {row['last_name']}: {e}")
            print(f"Raw response: {response}")
            return None
        except Exception as e:
            print(f"Error validating profile for {row['first_name']} {row['last_name']}: {e}")
            print(f"Profile data: {profile_data}")
            return None
    
    def _fix_profile_validation_issues(self, profile_data: Dict[str, Any]):
        """Fix common validation issues in profile data"""
        # Fix education section
        if 'personal_life' in profile_data and 'education' in profile_data['personal_life']:
            education = profile_data['personal_life']['education']
            
            # Fix missing time_preference
            if 'time_preference' not in education or education['time_preference'] is None:
                education['time_preference'] = "Evening"
            
            # Fix missing learning_style
            if 'learning_style' not in education or education['learning_style'] is None:
                education['learning_style'] = "Visual"
            
            # Fix current_studies None values
            if education.get('current_studies') is None:
                education['current_studies'] = ""
        
        # Fix professional_hierarchy section
        if 'professional' in profile_data and 'professional_hierarchy' in profile_data['professional']:
            hierarchy = profile_data['professional']['professional_hierarchy']
            
            # Fix direct_reports if it's not a list
            if 'direct_reports' in hierarchy and not isinstance(hierarchy['direct_reports'], list):
                hierarchy['direct_reports'] = []
            
            # Fix other list fields
            for field in ['peers', 'collaborators', 'mentees', 'mentors']:
                if field in hierarchy and not isinstance(hierarchy[field], list):
                    hierarchy[field] = []