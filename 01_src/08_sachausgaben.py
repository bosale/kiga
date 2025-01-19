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
logger = setup_logger('sachausgaben')

def load_structure() -> dict:
    """
    Load the material expenses structure from YAML file.
    
    Returns:
        dict: The structure configuration for material expenses
    """
    structure_file = Path(__file__).parent / "config" / "sachausgaben_structure.yaml"
    with open(structure_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def extract_sachausgaben(file_path: str | Path) -> pd.DataFrame:
    """
    Extract material expenses data from the Excel file.
    
    Args:
        file_path: Path to the Excel file to process
        
    Returns:
        pd.DataFrame: Extracted material expenses data
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
    
    # Find the year columns in row 8
    year_2022_col = None
    year_2023_col = None
    comment_col = None
    
    header_row = df.iloc[8]
    for col in range(len(header_row)):
        cell_value = str(header_row[col]).strip() if pd.notna(header_row[col]) else ''
        if '2022' in cell_value:
            year_2022_col = col
        elif '2023' in cell_value:
            year_2023_col = col
        elif 'KOMMENTAR' in cell_value.upper():
            comment_col = col
    
    # Find the start of the SACHAUSGABEN section
    start_row = None
    for idx in range(8, len(df)):
        row = df.iloc[idx]
        for col in range(len(row)):
            cell_value = str(row[col]).strip() if pd.notna(row[col]) else ''
            if 'II. SACHAUSGABEN' in cell_value:
                start_row = idx
                break
        if start_row is not None:
            break
    
    if start_row is None:
        raise ValueError("Could not find SACHAUSGABEN section")
    
    # Define the expected section headers from structure
    section_patterns = list(structure['II. SACHAUSGABEN'].keys())
    
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
    
    # Process each row after the SACHAUSGABEN section
    main_category = "II. SACHAUSGABEN"
    current_subcategory = None
    current_detail = None
    
    for idx in range(start_row, len(df)):
        row = df.iloc[idx]
        found_section = False
        
        description = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
        
        # Check for section headers
        for pattern in section_patterns:
            if pattern in description:
                current_subcategory = pattern
                current_detail = None
                found_section = True
                break
        
        # Process data rows if we have a current subcategory
        if current_subcategory and not found_section and description:
            try:
                value_2022 = row.iloc[year_2022_col] if pd.notna(row.iloc[year_2022_col]) else None
                value_2023 = row.iloc[year_2023_col] if pd.notna(row.iloc[year_2023_col]) else None
                
                if (isinstance(value_2022, (int, float)) or isinstance(value_2023, (int, float))) and description:
                    for item in structure['II. SACHAUSGABEN'][current_subcategory]['items']:
                        clean_desc = ' '.join(description.lower().split())
                        clean_item = ' '.join(item.lower().split())
                        
                        if clean_desc == clean_item:
                            current_detail = item
                            
                            if isinstance(value_2022, (int, float)):
                                data['year_2022'][current_detail] = value_2022
                                data['category'][current_detail] = main_category
                                data['subcategory'][current_detail] = current_subcategory
                                data['subcategory_desc'][current_detail] = structure['II. SACHAUSGABEN'][current_subcategory]['description']
                                data['detail'][current_detail] = current_detail
                            if isinstance(value_2023, (int, float)):
                                data['year_2023'][current_detail] = value_2023
                                data['category'][current_detail] = main_category
                                data['subcategory'][current_detail] = current_subcategory
                                data['subcategory_desc'][current_detail] = structure['II. SACHAUSGABEN'][current_subcategory]['description']
                                data['detail'][current_detail] = current_detail
                            if comment_col is not None and pd.notna(row.iloc[comment_col]):
                                data['comments'][current_detail] = str(row.iloc[comment_col])
                            break
            except (ValueError, TypeError) as e:
                logger.debug(f"Error processing row {idx}: {e}")
                continue
        
        # Stop when we reach the next major section
        if 'III.' in description:
            break
    
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
    logger.info(f"Extracted {len(result_df)} rows of material expenses data")
    
    return result_df

def process_sachausgaben_files(
    directory_path: str | Path,
    file_pattern: str = "*.xlsx",
    checkpoint_file: str = "processed_files_sachausgaben.json",
    debug_limit: int | None = None
) -> pd.DataFrame:
    """
    Process multiple Excel files containing material expenses data.
    
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
        extraction_function=extract_sachausgaben,
        file_pattern=file_pattern,
        checkpoint_file=checkpoint_file,
        debug_limit=debug_limit,
        process_type='sachausgaben',
        default_columns=default_columns
    )

if __name__ == "__main__":
    # Get the script's directory and construct relative path
    script_dir = Path(__file__).parent
    directory_path = script_dir.parent / "02_data" / "01_input"
    checkpoint_file = directory_path.parent / "processed_files_sachausgaben.json"
    
    # Set debug_limit to process only a few files (set to None for processing all files)
    debug_limit = 1
    
    # Set logging level to DEBUG
    logger.setLevel('DEBUG')
    
    try:
        results = process_sachausgaben_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        if results.empty:
            logger.warning("No data was extracted from the processed files")
            exit(1)
            
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results['source_file'].nunique()}")
        logger.info(f"Total material expense records: {len(results)}")
        
        # Save to CSV
        output_path = directory_path.parent / "02_output" / "kindergarten_sachausgaben.csv"
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise 