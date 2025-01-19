import pandas as pd
import os
from pathlib import Path
from utils import (
    find_sheet_with_content,
    process_multiple_files,
    setup_logger
)
import yaml

# Setup logger
logger = setup_logger('personalausgaben')

def load_structure() -> dict:
    """
    Load the personnel expenses structure from YAML file.
    
    Returns:
        dict: The structure configuration for personnel expenses
    """
    structure_file = Path(__file__).parent / "config" / "personalausgaben_structure.yaml"
    with open(structure_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def extract_personalausgaben(file_path: str | Path) -> pd.DataFrame:
    """
    Extract personnel expenses data from the Excel file.
    
    Args:
        file_path: Path to the Excel file to process
        
    Returns:
        pd.DataFrame: Extracted personnel expenses data with columns:
            - source_file: Name of processed file
            - category: Main category (I. PERSONALAUSGABEN)
            - subcategory: Section (e.g. BETREUUNGSPERSONAL)
            - subcategory_desc: Description of the subcategory
            - detail: Specific item details
            - value_2022: Value for year 2022
            - value_2023: Value for year 2023
            - comment: Additional comments if any
            
    Raises:
        ValueError: If required sections are not found in the file
    """
    logger.info(f"\nProcessing file: {file_path}")
    
    # Load the structure
    structure = load_structure()
    
    # Find the correct sheet
    target_sheet = find_sheet_with_content(file_path, 'A. AUSGABEN')
    logger.info(f"Found sheet: {target_sheet}")
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'A. AUSGABEN' found in {file_path}")
    
    # Read the full sheet
    df = pd.read_excel(
        file_path,
        sheet_name=target_sheet,
        header=None
    )
    
    # First find the year columns in row 8 (which we can see from the log)
    year_2022_col = None
    year_2023_col = None
    comment_col = None
    
    header_row = df.iloc[8]  # Use row 8 which contains the year headers
    for col in range(len(header_row)):
        cell_value = str(header_row[col]).strip() if pd.notna(header_row[col]) else ''
        if '2022' in cell_value:
            year_2022_col = col
            logger.debug(f"Found 2022 column at {col}: {cell_value}")
        elif '2023' in cell_value:
            year_2023_col = col
            logger.debug(f"Found 2023 column at {col}: {cell_value}")
        elif 'KOMMENTAR' in cell_value.upper():
            comment_col = col
            logger.debug(f"Found comment column at {col}: {cell_value}")
    
    logger.debug(f"Column positions - 2022: {year_2022_col}, 2023: {year_2023_col}, comment: {comment_col}")
    
    # Now find the start of the PERSONALAUSGABEN section
    start_row = None
    for idx in range(8, len(df)):  # Start after the header row
        row = df.iloc[idx]
        # Check all columns for the text
        for col in range(len(row)):
            cell_value = str(row[col]).strip() if pd.notna(row[col]) else ''
            if 'I. PERSONALAUSGABEN' in cell_value:
                start_row = idx
                logger.debug(f"Found PERSONALAUSGABEN section at row {idx}, column {col}: {cell_value}")
                break
        if start_row is not None:
            break
    
    if start_row is None:
        # Log the problematic rows for debugging
        logger.debug("\nChecking rows 8-15 for PERSONALAUSGABEN section:")
        for idx in range(8, min(16, len(df))):
            row_values = [str(val).strip() if pd.notna(val) else '' for val in df.iloc[idx]]
            logger.debug(f"Row {idx}: {row_values}")
        raise ValueError("Could not find PERSONALAUSGABEN section")
    
    # Define the expected section headers from structure
    section_patterns = list(structure['I. PERSONALAUSGABEN'].keys())
    
    # Initialize data dictionary
    data = {
        'source_file': Path(file_path).stem,
        'category': {},      # Will be "I. PERSONALAUSGABEN" for all entries
        'subcategory': {},   # Will store the main sections (1. BETREUUNGSPERSONAL etc.)
        'subcategory_desc': {}, # Will store the descriptions of subcategories
        'detail': {},        # Will store the specific items
        'year_2022': {},
        'year_2023': {},
        'comments': {}
    }
    
    # Process each row after the PERSONALAUSGABEN section
    main_category = "I. PERSONALAUSGABEN"
    current_subcategory = None
    current_detail = None
    
    # Add debug logging for structure
    logger.debug(f"Structure loaded: {structure['I. PERSONALAUSGABEN']}")
    
    for idx in range(start_row, len(df)):
        row = df.iloc[idx]
        found_section = False
        
        # Get the description from the third column (index 2)
        description = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
        logger.debug(f"Processing row {idx}: {description}")
        
        # Check for section headers
        for pattern in section_patterns:
            if pattern in description:
                current_subcategory = pattern
                current_detail = None  # Reset detail when new subcategory is found
                logger.debug(f"Found subcategory: {current_subcategory}")
                found_section = True
                break
        
        # Process data rows if we have a current subcategory
        if current_subcategory and not found_section and description:
            logger.debug(f"Description: '{description}'")
            logger.debug(f"Current subcategory: {current_subcategory}")
            logger.debug(f"Expected items: {structure['I. PERSONALAUSGABEN'][current_subcategory]['items']}")
            try:
                value_2022 = row.iloc[year_2022_col] if pd.notna(row.iloc[year_2022_col]) else None
                value_2023 = row.iloc[year_2023_col] if pd.notna(row.iloc[year_2023_col]) else None
                
                # Check if either value is numeric
                if (isinstance(value_2022, (int, float)) or isinstance(value_2023, (int, float))) and description:
                    # Log the values we're checking
                    logger.debug(f"Checking description '{description}' against items: {structure['I. PERSONALAUSGABEN'][current_subcategory]['items']}")
                    
                    # Check if this description matches any of the expected items
                    for item in structure['I. PERSONALAUSGABEN'][current_subcategory]['items']:
                        # Clean up the strings for comparison
                        clean_desc = ' '.join(description.lower().split())
                        clean_item = ' '.join(item.lower().split())
                        
                        if clean_desc == clean_item:
                            current_detail = item
                            logger.debug(f"Found matching item: {current_detail} with values 2022: {value_2022}, 2023: {value_2023}")
                            
                            if isinstance(value_2022, (int, float)):
                                data['year_2022'][current_detail] = value_2022
                                data['category'][current_detail] = main_category
                                data['subcategory'][current_detail] = current_subcategory
                                data['subcategory_desc'][current_detail] = structure['I. PERSONALAUSGABEN'][current_subcategory]['description']
                                data['detail'][current_detail] = current_detail
                            if isinstance(value_2023, (int, float)):
                                data['year_2023'][current_detail] = value_2023
                                data['category'][current_detail] = main_category
                                data['subcategory'][current_detail] = current_subcategory
                                data['subcategory_desc'][current_detail] = structure['I. PERSONALAUSGABEN'][current_subcategory]['description']
                                data['detail'][current_detail] = current_detail
                            if comment_col is not None and pd.notna(row.iloc[comment_col]):
                                data['comments'][current_detail] = str(row.iloc[comment_col])
                            break
            except (ValueError, TypeError) as e:
                logger.debug(f"Error processing row {idx}: {e}")
                continue
        
        # Stop when we reach the next major section
        if 'II.' in description:
            logger.debug(f"Found end of section at row {idx}")
            break
    
    # Log the collected data before converting to DataFrame
    logger.debug(f"Collected data before DataFrame conversion: {data}")
    
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
    
    result_df = pd.DataFrame(rows)
    logger.info(f"Extracted {len(result_df)} rows of personnel expenses data")
    
    if result_df.empty:
        logger.warning("No data was extracted from this file. Structure items:")
        for subcategory, details in structure['I. PERSONALAUSGABEN'].items():
            logger.warning(f"{subcategory}: {details['items']}")
    
    return result_df

def process_personalausgaben_files(
    directory_path: str | Path,
    file_pattern: str = "*.xlsx",
    checkpoint_file: str = "processed_files_personalausgaben.json",
    debug_limit: int | None = None
) -> pd.DataFrame:
    """
    Process multiple Excel files containing personnel expenses data.
    
    Args:
        directory_path: Path to directory containing Excel files
        file_pattern: Pattern to match Excel files
        checkpoint_file: Path to checkpoint file tracking processed files
        debug_limit: If set, limits the number of files to process
        
    Returns:
        pd.DataFrame: Combined data from all processed files
    """
    default_columns = ['source_file', 'category', 'subcategory', 'detail', 'value_2022', 'value_2023', 'comment']
    return process_multiple_files(
        directory_path=directory_path,
        extraction_function=extract_personalausgaben,
        file_pattern=file_pattern,
        checkpoint_file=checkpoint_file,
        debug_limit=debug_limit,
        process_type='personalausgaben',
        default_columns=default_columns
    )

if __name__ == "__main__":
    # Get the script's directory and construct relative path
    script_dir = Path(__file__).parent
    directory_path = script_dir.parent / "02_data" / "01_input"
    checkpoint_file = directory_path.parent / "processed_files_personalausgaben.json"
    
    # Set debug_limit to process only a few files (set to None for processing all files)
    debug_limit = 1
    
    # Set logging level to DEBUG
    logger.setLevel('DEBUG')
    
    try:
        results = process_personalausgaben_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        if results.empty:
            logger.warning("No data was extracted from the processed files")
            exit(1)
            
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results['source_file'].nunique()}")
        logger.info(f"Total personnel expense records: {len(results)}")
        
        # Save to CSV
        output_path = directory_path.parent / "02_output" / "kindergarten_personalausgaben.csv"
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise 