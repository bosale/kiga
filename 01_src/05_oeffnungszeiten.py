import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path
import json
from utils import (
    find_sheet_with_content, 
    get_processed_files, 
    update_checkpoint,
    handle_problematic_files,
    setup_logger
)

# Setup logger
logger = setup_logger('oeffnungszeiten')

def extract_oeffnungszeiten(file_path):
    """
    Extract opening times (Öffnungszeiten) from the kindergarten Excel file.
    Returns a DataFrame with group types and their opening hours.
    """
    logger.info(f"\nProcessing file: {file_path}")
    
    # Find the correct sheet using helper function
    target_sheet = find_sheet_with_content(file_path, 'D. ÖFFNUNGSZEITEN')
    logger.info(f"Found sheet: {target_sheet}")
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'ÖFFNUNGSZEITEN' found in {file_path}")
    
    # Read the full sheet
    df = pd.read_excel(
        file_path,
        sheet_name=target_sheet,
        header=None
    )
    
    # Find the starting row of Öffnungszeiten section
    start_row = None
    for idx, row in df.iterrows():
        if any('D. ÖFFNUNGSZEITEN' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            start_row = idx
            break
    
    if start_row is None:
        raise ValueError(f"Could not find 'ÖFFNUNGSZEITEN' section in {file_path}")
    
    logger.debug(f"Found Öffnungszeiten section at row: {start_row}")
    
    # Define the groups we want to extract
    target_groups = [
        'Kleinkindergruppe (Krippe)',
        'Familiengruppe 0 - 6',
        'Familiengruppe 2 - 6',
        'Familiengruppe 3 - 10, mit Teilhort',
        'Familiengruppe 3 - 10, ohne Teilhort',
        'Kindergartengruppe ganztags',
        'Kindergartengruppe halbtags',
        'Teilhortgruppe',
        'Hortgruppe',
        'Kindergruppe',
        'Hortkindergruppe',
        'Integrationskleinkindergruppe',
        'Integrationskindergartengruppe',
        'Heilpädagogische Kindergartengruppe',
        'Heilpädagogische Hortgruppe'
    ]
    
    # Initialize list to store the data
    data = []
    
    # Find the row containing column headers
    header_row = None
    group_col = None
    hours_col = None
    days_col = None
    hours_per_day_col = None
    time_range_col = None
    
    # Look for header row by searching for specific column headers
    for idx in range(start_row, min(start_row + 15, len(df))):
        row = df.iloc[idx]
        # Debug output for row contents
        logger.debug(f"Checking row {idx}: {row.tolist()}")
        
        # Check if this row contains our expected headers
        for col, val in enumerate(row):
            if pd.isna(val):
                continue
            val_str = str(val).upper().strip()
            
            # Look for key column identifiers
            if 'WOCHENTAG' in val_str:
                days_col = col
                header_row = idx
            elif 'STUNDEN' in val_str and not any(x in val_str for x in ['Ø', 'DURCHSCHNITT']):
                hours_per_day_col = col
                header_row = idx
            elif 'UHRZEIT' in val_str or ('VON' in val_str and 'BIS' in val_str):
                time_range_col = col
                header_row = idx
            elif 'Ø STUNDEN' in val_str or 'DURCHSCHNITT' in val_str:
                hours_col = col
    
    # After finding header row, look for group column
    if header_row is not None:
        # Look for group column by checking the next few rows
        for idx in range(header_row + 1, min(header_row + 5, len(df))):
            row = df.iloc[idx]
            for col, val in enumerate(row):
                if pd.notna(val) and str(val) in target_groups:
                    group_col = col
                    break
            if group_col is not None:
                break
    
    if header_row is None or group_col is None:
        logger.error("Header structure:")
        for idx in range(start_row, min(start_row + 15, len(df))):
            logger.error(f"Row {idx}: {df.iloc[idx].tolist()}")
        raise ValueError("Could not identify table structure")
    
    logger.debug(f"Found columns - Group: {group_col}, Hours: {hours_col}, Days: {days_col}, "
                f"Hours per day: {hours_per_day_col}, Time range: {time_range_col}")
    
    # Process each row after the header
    for idx in range(header_row + 1, len(df)):
        if pd.isna(df.iloc[idx, group_col]):
            continue
            
        group_name = str(df.iloc[idx, group_col])
        
        if group_name in target_groups:
            row_data = {
                'Gruppe': group_name,
                'Stunden_pro_Woche': df.iloc[idx, hours_col] if hours_col is not None and pd.notna(df.iloc[idx, hours_col]) else None,
                'Wochentage': df.iloc[idx, days_col] if days_col is not None and pd.notna(df.iloc[idx, days_col]) else None,
                'Stunden_pro_Tag': df.iloc[idx, hours_per_day_col] if hours_per_day_col is not None and pd.notna(df.iloc[idx, hours_per_day_col]) else None,
                'Oeffnungszeiten': df.iloc[idx, time_range_col] if time_range_col is not None and pd.notna(df.iloc[idx, time_range_col]) else None,
                'source_file': Path(file_path).stem
            }
            logger.debug(f"Found group: {group_name}")
            logger.debug(f"Row data: {row_data}")
            data.append(row_data)
    
    # Create DataFrame from the collected data
    result_df = pd.DataFrame(data)
    
    if len(result_df) == 0:
        raise ValueError("No Öffnungszeiten data found in the file")
    
    logger.info(f"Extracted {len(result_df)} rows of data")
    return result_df

def process_multiple_files(directory_path, file_pattern="*.xlsx", checkpoint_file="processed_files_oeffnungszeiten.json", debug_limit=None):
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
    Extracts Öffnungszeiten data from each file and logs problematic files.
    """
    # Get list of all Excel files in the directory
    file_paths = glob.glob(os.path.join(directory_path, file_pattern))
    
    if not file_paths:
        raise FileNotFoundError(f"No Excel files found in {directory_path}")
    
    # Limit files if in debug mode
    if debug_limit is not None:
        file_paths = file_paths[:debug_limit]
        logger.info(f"DEBUG MODE: Processing only {debug_limit} files")
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
                df_oeffnung = extract_oeffnungszeiten(file_path)
                all_results.append(df_oeffnung)
                
                logger.info(f"Successfully processed file: {file_name}")
                if debug_limit is None:
                    update_checkpoint(checkpoint_file, file_name)
            except Exception as e:
                error_message = str(e)
                error_type = type(e).__name__
                logger.error(f"Error processing {file_name}: {error_message}")
                problematic_files.append({
                    'file_name': file_name,
                    'error_type': error_type,
                    'error_description': error_message,
                    'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                continue
    
    # Handle problematic files
    handle_problematic_files(problematic_files, directory_path, 'oeffnungszeiten')
    
    # Combine results
    if not all_results:
        raise ValueError("No files were successfully processed")
    
    combined_df = pd.concat(all_results, ignore_index=True)
    return combined_df

if __name__ == "__main__":
    # Get the script's directory and construct relative path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.normpath(os.path.join(script_dir, "..", "02_data", "01_input"))
    checkpoint_file = os.path.join(os.path.dirname(directory_path), "processed_files_oeffnungszeiten.json")
    
    # Set debug_limit to process only a few files (set to None for processing all files)
    debug_limit = None
    
    try:
        results = process_multiple_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results['source_file'].nunique()}")
        logger.info(f"Total Öffnungszeiten records: {len(results)}")
        
        # Save to CSV
        output_path = os.path.join(os.path.dirname(directory_path), "02_output", "kindergarten_oeffnungszeiten.csv")
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error: {str(e)}") 