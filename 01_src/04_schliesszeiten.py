import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path
import json
from utils import find_sheet_with_content

def extract_schliesszeiten(file_path):
    """
    Extract closing times (Schliesszeiten) from the kindergarten Excel file.
    Returns a DataFrame with months and closing days for each kindergarten year.
    """
    # Read the Excel file
    print(f"\nProcessing file: {file_path}")
    
    # Find the correct sheet using helper function
    target_sheet = find_sheet_with_content(file_path, 'C. SCHLIESSZEITEN')
    print(f"Found sheet: {target_sheet}")
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'SCHLIESSZEITEN' found in {file_path}")
    
    # Read the full sheet
    df = pd.read_excel(
        file_path,
        sheet_name=target_sheet,
        header=None
    )
    
    # Find the starting row of Schliesszeiten section
    start_row = None
    for idx, row in df.iterrows():
        if any('C. SCHLIESSZEITEN' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            start_row = idx
            break
    
    if start_row is None:
        raise ValueError(f"Could not find 'SCHLIESSZEITEN' section in {file_path}")
    
    # Initialize lists to store the data
    data = []
    
    # Find the columns containing the kindergarten years
    year_cols = []
    year_row_idx = None
    
    # First, find the row containing "September"
    september_row = None
    for idx in range(start_row, min(start_row + 15, len(df))):  # Look within 15 rows after start
        row = df.iloc[idx]
        for col in range(len(row)):
            if pd.notna(row[col]) and 'SEPTEMBER' in str(row[col]).upper().strip():
                september_row = idx
                break
        if september_row is not None:
            break
    
    if september_row is None:
        print("\nCouldn't find September row. Context rows:")
        for i in range(max(0, start_row), min(start_row + 15, len(df))):
            print(f"Row {i}:", df.iloc[i].tolist())
        raise ValueError("Could not find row containing 'September'")
    
    # Look for kindergarten years in the 2-3 rows before September
    for row_offset in range(1, 4):
        check_row = df.iloc[september_row - row_offset]
        print(f"\nChecking row {september_row - row_offset} for years:")
        for col in range(len(check_row)):
            val = str(check_row[col]).strip() if pd.notna(check_row[col]) else ''
            print(f"Column {col}: {val}")
            if pd.notna(check_row[col]) and 'KINDERGARTENJAHR' in str(check_row[col]).upper():
                year_cols.append(col)
                year_row_idx = september_row - row_offset
    
    # If no exact matches found, try to find years in a more flexible way
    if not year_cols:
        print("\nNo exact matches found. Trying alternative search...")
        for row_offset in range(1, 4):
            check_row = df.iloc[september_row - row_offset]
            for col in range(len(check_row)):
                val = str(check_row[col]).strip() if pd.notna(check_row[col]) else ''
                # Look for patterns like "2022/2023" or "2022/23" or "22/23"
                if '/' in val and any(char.isdigit() for char in val):
                    year_cols.append(col)
                    year_row_idx = september_row - row_offset
                    print(f"Found potential year column: {col} with value: {val}")
    
    if not year_cols:
        print("\nContext rows:")
        for i in range(max(0, september_row - 4), min(september_row + 2, len(df))):
            print(f"Row {i}:", df.iloc[i].tolist())
        raise ValueError("No kindergarten years found in the file")
    
    # Get the months starting from September row
    months = []
    month_col = None
    
    # Find the column containing "September"
    for col in range(len(df.iloc[september_row])):
        if pd.notna(df.iloc[september_row, col]) and 'SEPTEMBER' in str(df.iloc[september_row, col]).upper():
            month_col = col
            break
    
    if month_col is None:
        raise ValueError("Could not find month column")
    
    # Get all 12 months starting from September
    for row in range(12):
        month = df.iloc[september_row + row, month_col]
        if pd.notna(month) and isinstance(month, str):
            months.append(month.strip())
    
    # Process each kindergarten year
    for year_col in year_cols:
        # Get the kindergarten year
        kg_year = str(df.iloc[year_row_idx, year_col]).strip()
        
        # Process each month
        for row_offset, month in enumerate(months):
            try:
                # Read closing days from the year column, not the month column
                closing_days = df.iloc[september_row + row_offset, year_col + 1]  # Add offset of 1 to get the value column
                
                # Only add entries where we have actual closing days
                if pd.notna(closing_days) and str(closing_days).strip() != '':
                    try:
                        closing_days = int(float(str(closing_days).strip()))
                        data.append({
                            'Kindergartenjahr': kg_year,
                            'Monat': month,
                            'Schliesstage': closing_days,
                            'source_file': Path(file_path).stem
                        })
                    except ValueError:
                        print(f"Warning: Could not convert '{closing_days}' to integer for {month} in {kg_year}")
            except Exception as e:
                print(f"Warning: Error processing {month} for {kg_year}: {str(e)}")
    
    # Create DataFrame from the collected data
    result_df = pd.DataFrame(data)
    
    if len(result_df) == 0:
        raise ValueError("No Schliesszeiten data found in the file")
    
    print(f"Found Schliesszeiten section at row: {start_row}")
    print(f"Found year columns at: {year_cols}")
    print(f"Detected months: {months}")
    
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
    Extracts Schliesszeiten data from each file and logs problematic files.
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
                df_schliess = extract_schliesszeiten(file_path)
                all_results.append(df_schliess)
                
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
        problems_path = os.path.join(os.path.dirname(directory_path), "problematic_files_schliesszeiten.csv")
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
    checkpoint_file = os.path.join(os.path.dirname(directory_path), "processed_files_schliesszeiten.json")
    
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
        print(f"Total Schliesszeiten records: {len(results)}")
        
        # Save to CSV
        output_path = os.path.join(os.path.dirname(directory_path), "02_output", "kindergarten_schliesszeiten.csv")
        results.to_csv(output_path, index=False)
        print(f"\nResults saved to: {output_path}")
        print(results.head())
        
    except Exception as e:
        print(f"Error: {str(e)}") 