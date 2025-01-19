import json
import os
from pathlib import Path
import pandas as pd

def get_processed_files(checkpoint_file):
    """Load the set of processed files from the checkpoint file."""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return set(json.load(f))
    return set()

def update_checkpoint(checkpoint_file, file_name):
    """Update the checkpoint file with a newly processed file."""
    processed_files = get_processed_files(checkpoint_file)
    processed_files.add(file_name)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
    
    with open(checkpoint_file, 'w') as f:
        json.dump(list(processed_files), f)

def handle_problematic_files(problematic_files, directory_path, script_name):
    """Save information about problematic files to a CSV."""
    if problematic_files:
        output_file = os.path.join(
            os.path.dirname(directory_path), 
            f"problematic_files_{script_name}.csv"
        )
        pd.DataFrame(problematic_files).to_csv(output_file, index=False) 