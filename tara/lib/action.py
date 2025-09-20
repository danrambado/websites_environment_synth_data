from langdetect import detect
import re
from openai import OpenAI
import os
from ollama import chat
from ollama import ChatResponse
from dotenv import load_dotenv
from pathlib import Path

import base64
from pathlib import Path
import logging

class Action:
    def __init__(self,model=None):
        load_dotenv()
        self.model=model
        self.message=None
        self.pattern=None
        self.origin_colum_name=None
        self.system_prompt=None
        self.output_prompt=None
        self.limit_characters_prompt=50000
        self.models={
            'o1': {'platform': 'OpenIA'},
            'o1-preview': {'platform': 'OpenIA'},
            'gpt-4o': {'platform': 'OpenIA'},
            'o1-mini': {'platform': 'OpenIA'},
            'gpt-4o-mini': {'platform': 'OpenIA'},
            'azure_ai/deepseek-r1': {'platform': 'OpenIA'},
            'anthropic/claude-3-7-sonnet-20250219': {'platform': 'OpenIA'}, 
            'anthropic/claude-3-5-sonnet-20241022': {'platform': 'OpenIA'}, 
            'openai/o3-mini-2025-01-31':{'platform': 'OpenIA'},
            'openai/o3-mini':{'platform': 'OpenIA'},
            'openai/o4-mini':{'platform': 'OpenIA'},
            'gemini/gemini-2.0-flash-thinking-exp':{'platform': 'OpenIA'},
            'gemini/gemini-2.0-flash-thinking-exp-01-21':{'platform': 'OpenIA'},
            'llmengine/llama-3-3-70b-instruct':{'platform': 'OpenIA'},
            'llama3': {'platform': 'Ollama'},
            'llama3.2': {'platform': 'Ollama'},
            'llama3.2:latest': {'platform': 'Ollama'},
            'llama3.2-vision': {'platform': 'Ollama'},
            'phi4': {'platform': 'Ollama'},
            'deepseek-r1:8b': {'platform': 'Ollama'},
            'research-grok-0217': {'platform': 'XIA'},
            'dummy':{'platform':'Dummy'}
            
        }


    def set_limit_characters_prompt(self,limit_characters_prompt):
        """
        Initializes the limit_characters_prompt.
        """
        self.limit_characters_prompt=limit_characters_prompt

    def set_model(self,model):
        """
        Initializes the model.
        """
        self.model=model

    def set_origin_column_name(self,origin_column_name):
        """
        Initializes the original column name to extract text from.
        """
        self.origin_column_name=origin_column_name
    
    def set_system_prompt(self,system_prompt):
        """
        Initializes the system prompt.
        """
        self.system_prompt=system_prompt

    def initialize_default_action(self,message="Hello, World!"):
        self.message=message

    def default_action(self,row):
        return self.message

    def initialize_regex(self,pattern,origin_colum_name):
        """
        Initializes the regular expression pattern and the original column name to extract text from.
        """
        self.pattern=pattern
        self.origin_colum_name=origin_colum_name

    def regex(self, row) -> str:
        """
        Extracts the first occurrence of a regular expression pattern in a text.
        """
        output = re.findall(self.pattern, row[self.origin_colum_name],re.DOTALL)
        return output[0].strip() if output else ""

    def detect_language(self,row) -> str:
        """
        Detects the language of a given text using the polyglot library.

        Args:
            text (str): The text to analyze.

        Returns:
            str: The ISO 639-1 language code or full name if detection succeeds,
                or 'Unknown' if the language cannot be identified.
        """
        try:
            # Create a language detector for the given text
            detector =detect(self.origin_colum_name)
            # Return the detected language code (ISO 639-1) or language name
            return detector  # Use .name for the full language name
        except Exception as e:
            # Handle cases where the language cannot be determined
            return "Unknown"    

    def contains_code(self,row) -> bool:
        """
        Detects if the given text contains code-related patterns.
        
        Args:
            text (str): The text to analyze.

        Returns:
            bool: True if code-related patterns are detected, False otherwise.
        """

        if "```" in row[self.origin_colum_name]:
                return True

        # Common keywords or statements in various languages
        # Convert the list of languages to a regex pattern
        languages = ["python", "java", "cpp", "c++", "csharp", "c#", "javascript", "html", "css", "sql", "bash", "php", "ruby", "perl", "swift",
                 "kotlin", "typescript", "rust", "lua"]
        lang_pattern = re.compile('|'.join(map(re.escape, languages)), re.IGNORECASE)

        # Use the regex pattern to search for a match in the input string
        if re.search(lang_pattern, row[self.origin_colum_name]):
            return True
        else:
            return False
    def set_output_prompt(self,output_prompt):
        self.output_prompt = output_prompt

    def get_output_prompt(self):
        return self.output_prompt
    
    def get_output_prompt_xml(self):
        prompt=''
        for key, value in self.output_prompt.items():
            prompt += f"<{key}>{value}</{key}>\n"
        return prompt
        
    def prompt(self, prompt: str, row: dict=None,schema:dict=None) -> str:
        if row!=None:
            for key, value in row.items():
                prompt = prompt.replace(f"{{{key}}}", str(value))
        
        if len(prompt) > self.limit_characters_prompt:
            error_message = f"Limit Prompt Error ({self.limit_characters_prompt}): {prompt}"
            logging.error(error_message)
            return error_message

        if self.model in self.models:
            platform = self.models[self.model]['platform']
            logging.info(f"Prompt model|{self.model}|{platform}:{prompt}")

            if platform == 'OpenIA':
                response= self.promptOpenIA(prompt,schema=schema)
            elif platform == 'XIA':
                response= self.promptXIA(prompt)
            elif platform == 'Ollama':
                response= self.promptOllama(prompt)
            elif platform == 'Dummy':
                response= self.promptDummy(prompt)
            else:
                logging.error("Platform not found.")
                return "Platform not found."
            
            logging.info(f"Response model|{self.model}|{platform}:{response}")
            return response

        logging.error("Model not found.")
        return "Model not found."


    def promptOpenIA(self, prompt: str,schema:dict=None) -> str:
        try:
            # Create a new OpenAI client
            client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"), base_url=os.environ.get("END_POINT"))

            client_id=os.environ.get("CLIENT_ID")
            project_id=os.environ.get("PROJECT_ID")
            messages=[{
                        "role": "user",
                        "content": prompt,
                    }]
            
            if schema!=None:
                # Extract name from schema if it exists, otherwise use a default
                schema_name = schema.get("name", "default_schema")
                # Remove name from schema as it should be at json_schema level
                if "name" in schema:
                    schema = {k: v for k, v in schema.items() if k != "name"}
                
                response_format = {
                    "type": "json_schema",
                    "json_schema": {
                        "name": schema_name,
                        "schema": schema
                    }
                }
            else:
                response_format=None
            #print(response_format)
            completion = client.chat.completions.create(
                model=self.model,
                messages=messages,
                response_format=response_format,
                user=client_id,
                extra_body={
                "metadata":{
                        "tags": [f"project:{project_id}"]
                    }
                },
                
            )
            
            return completion.choices[0].message.content
        except Exception as e:
            return f"OpenIA call error: {str(e)}"
        
    def promptXIA(self, prompt: str) -> str:
        try:
            # Create a new XIA client
            client = OpenAI(api_key=os.environ.get("XAI_API_KEY"), base_url=os.environ.get("XAI_END_POINT"))
            messages=[]
            if self.system_prompt:
                messages=[
                    {
                        "role": "system",
                        "content": self.system_prompt,
                    }]
            messages.append({
                        "role": "user",
                        "content": prompt,
                    })
            

            completion = client.chat.completions.create(
                model=self.model,
                messages=messages
            )

            return completion.choices[0].message.content
        except Exception as e:
            return f"XIA call error: {str(e)}"
        

    def promptOllama(self, prompt: str) -> str:
        try:
            # Call the model and classify the prompt
            response: ChatResponse = chat(model=self.model, messages=[
            {
                'role': 'user',
                'content': prompt,
            },
            ])

            return response.message.content
        except Exception as e:
            return f"Ollama call error: {str(e)}"
    

    def promptDummy(self, prompt: str) -> str:
        return prompt


    
