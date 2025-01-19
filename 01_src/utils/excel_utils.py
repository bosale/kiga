import pandas as pd
import os
import logging
from pathlib import Path
import yaml

def find_sheet_with_content(file_path, search_text, nrows=500):
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
        # Skip the INFORMATION sheet
        if sheet_name.upper() == "INFORMATION":
            continue
            
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

def extract_section_data(
    df: pd.DataFrame,
    section_identifier: str,
    structure: dict,
    file_path: str | Path,
    logger: logging.Logger,
    header_row_index: int = 8,
    year_2022_col: int | None = None,
    year_2023_col: int | None = None,
    comment_col: int | None = None
) -> pd.DataFrame:
    """
    Extract data for a specific section (Personnel or Material expenses) from DataFrame.
    
    Args:
        df: DataFrame containing the data
        section_identifier: Section identifier (e.g., 'I.' for Personnel, 'II.' for Material)
        structure: Structure dictionary from YAML config
        file_path: Path to the source file
        logger: Logger instance
        header_row_index: Index of the header row (default: 8)
        year_2022_col: Column index for year 2022 (default: None)
        year_2023_col: Column index for year 2023 (default: None)
        comment_col: Column index for comments (default: None)
        
    Returns:
        pd.DataFrame: Extracted data
    """
    # If year columns are not provided, find them in the header row
    if any(col is None for col in [year_2022_col, year_2023_col, comment_col]):
        header_row = df.iloc[header_row_index]
        
        if year_2022_col is None or year_2023_col is None or comment_col is None:
            for col in range(len(header_row)):
                cell_value = str(header_row[col]).strip() if pd.notna(header_row[col]) else ''
                if year_2022_col is None and '2022' in cell_value:
                    year_2022_col = col
                elif year_2023_col is None and '2023' in cell_value:
                    year_2023_col = col
                elif comment_col is None and 'KOMMENTAR' in cell_value.upper():
                    comment_col = col

        if any([year_2022_col is None, year_2023_col is None, comment_col is None]):
            logger.warning("Using default column indices as not all columns were found in header")
            year_2022_col = 6 if year_2022_col is None else year_2022_col
            year_2023_col = 7 if year_2023_col is None else year_2023_col
            comment_col = 8 if comment_col is None else comment_col

    logger.debug(f"Using columns - 2022: {year_2022_col}, 2023: {year_2023_col}, comment: {comment_col}")

    # Find the start of the section - modified to handle all section types
    start_row = None
    
    # Get the section key from the structure instead of hardcoding
    section_key = list(structure.keys())[0]  # Get the full section name from structure
    logger.debug(f"Looking for section with key: {section_key}")
    
    # Look for the section header through the entire DataFrame to find the start row
    for idx in range(header_row_index, len(df)):  # Start from row 8 but search through all rows
        row = df.iloc[idx]
        for col in range(len(row)):
            cell_value = str(row[col]).strip() if pd.notna(row[col]) else ''
            # Check for both exact match and partial match
            if section_key in cell_value or f'{section_identifier}.' in cell_value:
                start_row = idx
                break
        if start_row is not None:
            break
    
    if start_row is None:
        logger.error(f"Section {section_identifier} not found in file")
        logger.debug("Available sections in structure:")
        logger.debug(structure.keys())
        raise ValueError(f"Could not find section {section_identifier}")
    else:
        logger.debug(f"Found start_row {start_row} at row {start_row}")
    
    # Use the actual section key from structure
    structure_key = section_key
    section_patterns = list(structure[structure_key].keys())
    
    # Initialize data dictionary
    data = {
        'source_file': Path(file_path).stem,
        'category': {},
        'subcategory': {},
        'subcategory_desc': {},
        'detail': {},
        'year_2022': {},
        'year_2023': {},
        'comments': {}
    }
    
    # Process each row after the section
    main_category = structure_key
    current_subcategory = None
    current_detail = None
    
    for idx in range(start_row, len(df)):
        row = df.iloc[idx]
        found_section = False
        
        description = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
        
        # Check for next major section to stop processing
        try:
            # Try to extract the section number and compare
            if description and description[0].isdigit():
                current_section = int(section_identifier[0])  # Get the current section number
                next_section = current_section + 1
                if description.startswith(f"{next_section}."):
                    break
        except (IndexError, ValueError):
            # If we can't parse the section number, continue processing
            pass
            
        # Check for section headers
        for pattern in section_patterns:
            # Make the pattern matching more flexible for this specific case
            if (description.strip() == pattern.strip() or
                description.strip() in pattern.strip() or
                pattern.strip() in description.strip()):
                current_subcategory = pattern
                current_detail = None
                found_section = True
                logger.debug(f"Found subsection: {pattern} matching description: {description}")
                break
        
        # Process data rows if we have a current subcategory
        if current_subcategory and not found_section and description:
            try:
                value_2022 = row.iloc[year_2022_col] if pd.notna(row.iloc[year_2022_col]) else None
                value_2023 = row.iloc[year_2023_col] if pd.notna(row.iloc[year_2023_col]) else None
                
                if (isinstance(value_2022, (int, float)) or isinstance(value_2023, (int, float))) and description:
                    # Make item matching more flexible
                    clean_desc = ' '.join(description.lower().split())
                    for item in structure[structure_key][current_subcategory]['items']:
                        clean_item = ' '.join(item.lower().split())
                        # More flexible matching for this case
                        if (clean_desc == clean_item or
                            clean_desc in clean_item or
                            clean_item in clean_desc):
                            current_detail = item
                            logger.debug(f"Found matching item: {item} for description: {description}")
                            
                            if isinstance(value_2022, (int, float)):
                                data['year_2022'][current_detail] = value_2022
                                data['category'][current_detail] = main_category
                                data['subcategory'][current_detail] = current_subcategory
                                data['subcategory_desc'][current_detail] = structure[structure_key][current_subcategory]['description']
                                data['detail'][current_detail] = current_detail
                            if isinstance(value_2023, (int, float)):
                                data['year_2023'][current_detail] = value_2023
                                data['category'][current_detail] = main_category
                                data['subcategory'][current_detail] = current_subcategory
                                data['subcategory_desc'][current_detail] = structure[structure_key][current_subcategory]['description']
                                data['detail'][current_detail] = current_detail
                            if comment_col is not None and pd.notna(row.iloc[comment_col]):
                                data['comments'][current_detail] = str(row.iloc[comment_col])
                            break
            except (ValueError, TypeError) as e:
                logger.debug(f"Error processing row {idx}: {e}")
                continue
    
    # After finding the section:
    if start_row is not None:
        logger.debug("Found main section, looking at content:")
        # Print the next 10 rows after the section start
        for i in range(start_row, min(start_row + 10, len(df))):
            row = df.iloc[i]
            desc = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
            val_2022 = row.iloc[year_2022_col] if pd.notna(row.iloc[year_2022_col]) else None
            val_2023 = row.iloc[year_2023_col] if pd.notna(row.iloc[year_2023_col]) else None
            logger.debug(f"Row {i}: Description: {desc}, 2022: {val_2022}, 2023: {val_2023}")
    
    # Convert to DataFrame
    rows = []
    for label in set(list(data['year_2022'].keys()) + list(data['year_2023'].keys())):
        row = {
            'source_file': data['source_file'],
            'category': data['category'].get(label, ''),
            'subcategory': data['subcategory'].get(label, ''),
            'subcategory_desc': data['subcategory_desc'].get(label, ''),
            'detail': data['detail'].get(label, ''),
            'value_2022': data['year_2022'].get(label),
            'value_2023': data['year_2023'].get(label),
            'comment': data['comments'].get(label, '')
        }
        rows.append(row)
    
    if not rows:
        logger.warning(f"No data was extracted from {file_path}")
        raise ValueError(f"No data extracted from {file_path}")
    
    logger.debug(f"Extracted {len(rows)} rows of data")
    return pd.DataFrame(rows) 

def load_structure(config_file: str) -> dict:
    """
    Load a structure configuration from YAML file.
    
    Args:
        config_file: Name of the YAML config file to load
        
    Returns:
        dict: The structure configuration
    """
    structure_file = Path(__file__).parent.parent / "config" / config_file
    with open(structure_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) 