import pandas as pd
import multiprocessing
from rich.console import Console
from rich.progress import track
from tara.lib.action import Action
import sys

class Pipeline:
    def __init__(self):
        self.console = Console()
        self.df = None

    def read_csv(self):
        self.console.log("Reading CSV...")
        self.df = pd.read_csv(self.csv_file_input)
        self.console.log(f"Total number of rows: {len(self.df)}")

    def execute_action(self, action, column, parallel=True):
        # Executes an action on the DataFrame, either in parallel or sequentially
        mode = "parallel" if parallel else "sequential"
        self.console.log(f"Categorizing {action.__name__} (in {mode})...")
        rows = self.df.to_dict(orient='records')
        
        if parallel:
            with multiprocessing.Pool() as pool:
                values = list(
                    track(
                        pool.imap(action, rows),
                        total=len(self.df),
                        description=f"Processing with {action.__name__}..."
                    )
                )
        else:
            values = list(
                track(
                    map(action, rows),
                    total=len(self.df), 
                    description=f"Processing with {action.__name__}..."
                )
            )
            
        self.df[column] = values
    
    def post_execute_action(self, action:Action, column):
        if action.get_output_prompt()!=None:
            for key, value in action.get_output_prompt().items():
                regex = r"<"+key+">(.*?)</"+key+">"
                self.execute_regex_action(regex, column, key, action)

    def execute_regex_action(self, regex, column_input, column_output, action):
        action.initialize_regex(regex, column_input)
        self.execute_action(action.regex,column_output)

    def save_csv(self):
        # Saves the DataFrame to a CSV file.
        self.console.log(f"Saving DataFrame to {self.csv_file_output}...")
        self.df.to_csv(self.csv_file_output, index=False)
        self.console.log("Script completed successfully!")

    def process(self):
        # Orchestrates the pipeline by reading the input CSV file, executing the actions in parallel,
        self.console.log("Starting script...")
        self.read_csv()

        
        # Example of how to use the Action class to process the data.
        """
        action = Action()

        action.initialize_default_action("Hello, World!")
        self.execute_action(action.default_action, 'greeting')

        action.set_origin_column_name('PROMPT')
        self.execute_action(action.detect_language, 'from_language')
        
        action.set_origin_column_name('RESPONSE')
        self.execute_action(action.contains_code, 'contains_code')
                
        action.initialize_regex(r"```(.*?)```", 'RESPONSE')
        self.execute_action(action.regex, 'code_snippet')
        """

        # Save the results to a new csv
        self.save_csv()

if __name__ == '__main__':
    orchestrator = Pipeline()
    orchestrator.process()