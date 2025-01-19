import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path
import json
from utils import find_sheet_with_content

def extract_zusatzangaben(file_path):
    """
    Extract additional information (Zusatzangaben) from the kindergarten Excel file.
    Returns a DataFrame with Name_Eintrag, Eintrag, and Erlaeuterung.
    """
    # Read the Excel file
    xl = pd.ExcelFile(file_path)
    print(file_path)
    
    # Find the correct sheet by checking content
    target_sheet = find_sheet_with_content(file_path, 'ZUSATZANGABEN')
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'ZUSATZANGABEN' found in {file_path}")
    
    # Read the entire sheet
    df = pd.read_excel(
        file_path,
        sheet_name=target_sheet,
        header=None
    )
    
    # Find the starting row of Zusatzangaben section
    start_row = None
    for idx, row in df.iterrows():
        if any('ZUSATZANGABEN' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            start_row = idx
            break
    
    if start_row is None:
        raise ValueError(f"Could not find 'ZUSATZANGABEN' section in {file_path}")
    
    # Initialize lists to store the data
    data = []
    
    # Process rows after the ZUSATZANGABEN header
    current_row = start_row + 2  # Skip header row
    while current_row < len(df):
        row = df.iloc[current_row]
        
        # Check if we've reached the end of the section
        if any('EINMALZAHLUNGEN' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            break
        
        # Get values from specific columns (A, C, F)
        name_eintrag = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
        eintrag = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
        erlaeuterung = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else None
        
        # Only add row if there's actual content
        if name_eintrag and name_eintrag != 'nan' and name_eintrag != '-':
            data.append({
                'Name_Eintrag': name_eintrag,
                'Eintrag': eintrag if eintrag and eintrag != 'nan' else None,
                'Erlaeuterung': erlaeuterung if erlaeuterung and erlaeuterung != 'nan' else None,
                'source_file': Path(file_path).stem
            })
        
        current_row += 1
    
    # Create DataFrame from the collected data
    result_df = pd.DataFrame(data)
    
    # Clean up the data - replace NaN with None
    result_df = result_df.replace({np.nan: None})
    
    if len(result_df) == 0:
        raise ValueError("No Zusatzangaben data found in the file")
    
    return result_df

def get_processed_files(checkpoint_file):
    """Read the checkpoint file containing already processed files"""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return set(json.load(f))
    return set()

def update_checkpoint(checkpoint_file, processed_file):
    """Update the checkpoint file with newly processed file"""
    processed_files = get_processed_files(checkpoint_file)
    processed_files.add(processed_file)
    with open(checkpoint_file, 'w') as f:
        json.dump(list(processed_files), f)

def process_multiple_files(directory_path, file_pattern="*.xlsx", checkpoint_file="processed_files.json", debug_limit=None):
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
    Extracts Zusatzangaben data from each file and logs problematic files.
    """
    # Get list of all Excel files in the directory
    file_paths = glob.glob(os.path.join(directory_path, file_pattern))
    
    if not file_paths:
        raise FileNotFoundError(f"No Excel files found in {directory_path}")
    
    # Limit files if in debug mode
    if debug_limit is not None:
        file_paths = file_paths[:debug_limit]
        print(f"DEBUG MODE: Processing only {debug_limit} files (ignoring checkpoints)")
        processed_files = set()
    else:
        processed_files = get_processed_files(checkpoint_file)
    
    # Initialize lists for results and problematic files
    all_results = []
    problematic_files = []
    
    # Process each file
    for file_path in file_paths:
        file_name = Path(file_path).name
        if debug_limit is not None or file_name not in processed_files:
            try:
                df_zusatz = extract_zusatzangaben(file_path)
                all_results.append(df_zusatz)
                
                print(f"Successfully processed file: {file_name}")
                if debug_limit is None:
                    update_checkpoint(checkpoint_file, file_name)
            except Exception as e:
                error_message = str(e)
                error_type = type(e).__name__
                print(f"Error processing {file_name}: {error_message}")
                problematic_files.append({
                    'file_name': file_name,
                    'error_type': error_type,
                    'error_description': error_message,
                    'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                continue
    
    # Save problematic files to CSV if any exist
    if problematic_files:
        problems_df = pd.DataFrame(problematic_files)
        problems_path = os.path.join(os.path.dirname(directory_path), "problematic_files_zusatz.csv")
        problems_df = problems_df[[
            'file_name', 'error_type', 'error_description', 'timestamp'
        ]].sort_values('timestamp', ascending=False)
        problems_df.to_csv(problems_path, index=False)
        print(f"\nProblematic files logged to: {problems_path}")
        print(f"Number of problematic files: {len(problematic_files)}")
    
    # Combine results
    if not all_results:
        raise ValueError("No files were successfully processed")
    
    combined_df = pd.concat(all_results, ignore_index=True)
    return combined_df

def clear_checkpoints(checkpoint_file="processed_files.json"):
    """Clear the checkpoint file to start fresh"""
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        print("Checkpoint file cleared.")

if __name__ == "__main__":
    # Get the script's directory and construct relative path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.normpath(os.path.join(script_dir, "..", "02_data", "01_input"))
    checkpoint_file = os.path.join(os.path.dirname(directory_path), "processed_files_zusatz.json")
    
    # Set debug_limit to process only a few files (set to None for processing all files)
    debug_limit = None
    
    try:
        results = process_multiple_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        print("\nExtracted Data Summary:")
        print(f"Total files processed: {results['source_file'].nunique()}")
        print(f"Total Zusatzangaben records: {len(results)}")
        
        # Save to CSV
        output_path = os.path.join(os.path.dirname(directory_path), "02_output", "kindergarten_zusatzangaben.csv")
        results.to_csv(output_path, index=False)
        print(f"\nResults saved to: {output_path}")
        print(results.head())
        
    except Exception as e:
        print(f"Error: {str(e)}")
