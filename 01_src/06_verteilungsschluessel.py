import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path
from utils import (
    find_sheet_with_content,
    get_processed_files,
    update_checkpoint,
    handle_problematic_files,
    setup_logger
)

# Setup logger
logger = setup_logger('verteilungsschluessel')

def extract_verteilungsschluessel(file_path):
    """
    Extract Verteilungsschlüssel data from the Excel file.
    Specifically looks for the section 'C. VERTEILUNGSSCHLÜSSEL KINDERGARTEN/KINDERGRUPPE UND HORT'
    """
    logger.info(f"\nProcessing file: {file_path}")
    
    # Find the correct sheet (should be the same as Deckblatt)
    target_sheet = find_sheet_with_content(file_path, 'DECKBLATT')
    logger.info(f"Found sheet: {target_sheet}")
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'DECKBLATT' found in {file_path}")
    
    # Read the full sheet
    df = pd.read_excel(
        file_path,
        sheet_name=target_sheet,
        header=None
    )
    
    # Find the starting row of Verteilungsschlüssel section
    start_row = None
    for idx, row in df.iterrows():
        if any('C. VERTEILUNGSSCHLÜSSEL' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            start_row = idx
            break
    
    if start_row is None:
        raise ValueError(f"Could not find 'Verteilungsschlüssel' section in {file_path}")
    
    # Initialize data dictionary
    data = {
        'source_file': Path(file_path).stem,
        'kindergarten_2022': None,
        'kindergarten_2023': None,
        'kindergarten_2024': None,
        'hort_2022': None,
        'hort_2023': None,
        'hort_2024': None
    }
    
    # Look for the data rows
    for idx in range(start_row, min(start_row + 10, len(df))):
        row = df.iloc[idx]
        
        # Look for year rows
        for col in range(len(row)):
            cell_value = str(row[col]).strip() if pd.notna(row[col]) else ''
            
            # Check for year identifiers
            if cell_value == '2022':
                # Get values from the columns
                kg_col = None
                hort_col = None
                
                # Find the columns with percentages
                for search_col in range(len(df.iloc[idx])):
                    if pd.notna(df.iloc[idx-1, search_col]):
                        header = str(df.iloc[idx-1, search_col]).strip()
                        if 'Kindergarten' in header:
                            kg_col = search_col
                        elif 'Hort' in header:
                            hort_col = search_col
                
                if kg_col is not None:
                    data['kindergarten_2022'] = df.iloc[idx, kg_col]
                if hort_col is not None:
                    data['hort_2022'] = df.iloc[idx, hort_col]
                    
            elif cell_value == '2023':
                if kg_col is not None:
                    data['kindergarten_2023'] = df.iloc[idx, kg_col]
                if hort_col is not None:
                    data['hort_2023'] = df.iloc[idx, hort_col]
                    
            elif cell_value == '2024':
                if kg_col is not None:
                    data['kindergarten_2024'] = df.iloc[idx, kg_col]
                if hort_col is not None:
                    data['hort_2024'] = df.iloc[idx, hort_col]
    
    # Convert to DataFrame
    result_df = pd.DataFrame([data])
    
    # Convert percentage values to floats
    for col in result_df.columns:
        if col != 'source_file':
            result_df[col] = pd.to_numeric(result_df[col].str.rstrip('%').astype(float) / 100 
                                         if isinstance(result_df[col].iloc[0], str) 
                                         else result_df[col])
    
    logger.info(f"Extracted data: {result_df.to_dict('records')[0]}")
    return result_df

def process_multiple_files(directory_path, file_pattern="*.xlsx", checkpoint_file="processed_files_verteilungsschluessel.json", debug_limit=None):
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
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
                df_verteilung = extract_verteilungsschluessel(file_path)
                all_results.append(df_verteilung)
                
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
    handle_problematic_files(problematic_files, directory_path, 'verteilungsschluessel')
    
    # Combine results
    if not all_results:
        raise ValueError("No files were successfully processed")
    
    combined_df = pd.concat(all_results, ignore_index=True)
    return combined_df

if __name__ == "__main__":
    # Get the script's directory and construct relative path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.normpath(os.path.join(script_dir, "..", "02_data", "01_input"))
    checkpoint_file = os.path.join(os.path.dirname(directory_path), "processed_files_verteilungsschluessel.json")
    
    # Set debug_limit to process only a few files (set to None for processing all files)
    debug_limit = 1
    
    try:
        results = process_multiple_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results['source_file'].nunique()}")
        logger.info(f"Total Verteilungsschlüssel records: {len(results)}")
        
        # Save to CSV
        output_path = os.path.join(os.path.dirname(directory_path), "02_output", "kindergarten_verteilungsschluessel.csv")
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error: {str(e)}") 