import pandas as pd
import os

def find_sheet_with_content(file_path, search_text, nrows=50):
    """
    Find the first sheet in an Excel file that contains the specified text.
    
    Args:
        file_path (str): Path to the Excel file
        search_text (str): Text to search for in the sheet
        nrows (int): Number of rows to preview in each sheet (default: 50)
    
    Returns:
        str: Name of the sheet containing the text, or None if not found
    """
    xl = pd.ExcelFile(file_path)
    
    for sheet_name in xl.sheet_names:
        # Read first few rows to check for the search text
        preview_df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            nrows=nrows
        )
        # Convert values to string and check if search text exists
        if any(search_text in str(val).upper() 
               for val in preview_df.values.flatten() 
               if pd.notna(val)):
            return sheet_name
    
    return None

def process_multiple_files(
    directory_path, 
    extraction_function,
    file_pattern="*.xlsx", 
    checkpoint_file="processed_files.json", 
    debug_limit=None,
    process_type="generic",
    default_columns=None
):
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
    
    Args:
        directory_path: Path to directory containing files to process
        extraction_function: Function to extract data from each file
        file_pattern: Pattern to match files (default: "*.xlsx")
        checkpoint_file: Path to checkpoint file (default: "processed_files.json")
        debug_limit: Limit number of files to process (default: None)
        process_type: Type of processing for logging (default: "generic")
        default_columns: Default columns for empty DataFrame (default: None)
    """
    import glob
    import logging
    from pathlib import Path
    from .checkpoint_utils import get_processed_files, update_checkpoint, handle_problematic_files
    
    # Get list of all Excel files in the directory
    file_paths = glob.glob(os.path.join(directory_path, file_pattern))
    
    if not file_paths:
        raise FileNotFoundError(f"No Excel files found in {directory_path}")
    
    # Limit files if in debug mode
    if debug_limit is not None:
        file_paths = file_paths[:debug_limit]
        logging.info(f"DEBUG MODE: Processing only {debug_limit} files")
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
                df_result = extraction_function(file_path)
                all_results.append(df_result)
                
                logging.info(f"Successfully processed file: {file_name}")
                if debug_limit is None:
                    update_checkpoint(checkpoint_file, file_name)
            except Exception as e:
                error_message = str(e)
                error_type = type(e).__name__
                logging.error(f"Error processing {file_name}: {error_message}")
                problematic_files.append({
                    'file_name': file_name,
                    'error_type': error_type,
                    'error_description': error_message,
                    'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                continue
    
    # Handle problematic files
    handle_problematic_files(problematic_files, directory_path, process_type)
    
    # Combine results
    if not all_results:
        raise ValueError("No files were successfully processed")
    
    combined_df = pd.concat(all_results, ignore_index=True)
    
    # Add validation for empty DataFrame
    if combined_df.empty:
        logging.warning(f"No data was extracted from any {process_type} files")
        return pd.DataFrame(columns=default_columns if default_columns else ['source_file'])
        
    return combined_df 