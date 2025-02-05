"""
Main script for extracting kindergarten data from Excel files.
"""

import os
import glob
from pathlib import Path
import pandas as pd
from typing import Optional

from extractors.data_extractor import KindergartenDataExtractor
from utils.checkpoint_manager.checkpoint_handler import CheckpointManager

def process_multiple_files(
    directory_path: str,
    file_pattern: str = "*.xlsx",
    checkpoint_file: Optional[str] = "processed_files.json"
) -> pd.DataFrame:
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
    """
    # Get list of all Excel files in the directory
    file_paths = glob.glob(os.path.join(directory_path, file_pattern))
    
    if not file_paths:
        raise FileNotFoundError(f"No Excel files found in {directory_path}")
    
    # Initialize checkpoint manager if checkpoint file is provided
    checkpoint_mgr = CheckpointManager(checkpoint_file) if checkpoint_file else None
    processed_files = checkpoint_mgr.get_processed_files() if checkpoint_mgr else set()
    
    # Process each file
    all_results = []
    
    for file_path in file_paths:
        file_name = Path(file_path).name
        if not checkpoint_mgr or file_name not in processed_files:
            try:
                # Extract data from both sections
                extractor = KindergartenDataExtractor(file_path)
                combined_df = extractor.extract_all_sections()
                all_results.append(combined_df)
                
                print(f"Successfully processed new file: {file_name}")
                if checkpoint_mgr:
                    checkpoint_mgr.update_checkpoint(file_name)
                    
            except Exception as e:
                print(f"Error processing {file_name}: {str(e)}")
                continue
    
    # Combine all results
    if not all_results:
        raise ValueError("No files were successfully processed")
    
    return pd.concat(all_results, ignore_index=True)

if __name__ == "__main__":
    # Get the script's directory and construct relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.normpath(os.path.join(script_dir, "..", "02_data", "01_input"))
    checkpoint_file = os.path.join(os.path.dirname(directory_path), "processed_files_deckblatt.json")
    
    try:
        # Process files
        combined_results = process_multiple_files(
            directory_path=directory_path,
            checkpoint_file=checkpoint_file
        )
        
        # Print summary
        print("\nExtracted Data Summary:")
        print(f"Total files processed: {combined_results['source_file'].nunique()}")
        print(f"Total records: {len(combined_results)}")
        
        # Save to CSV
        output_path = os.path.join(os.path.dirname(directory_path), "02_output", "kindergarten_deckblatt.csv")
        combined_results.to_csv(output_path, index=False)
        print(f"\nResults saved to: {output_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}") 