import pandas as pd
import os
from pathlib import Path
from utils import (
    find_sheet_with_content,
    process_multiple_files,
    setup_logger,
    extract_section_data,
    load_structure
)
import yaml

# Setup logger
logger = setup_logger('personalausgaben')


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
    
    structure = load_structure()
    logger.debug(f"Loaded structure: {structure}")
    
    target_sheet = find_sheet_with_content(file_path, 'A. AUSGABEN')
    logger.info(f"Found sheet: {target_sheet}")
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'A. AUSGABEN' found in {file_path}")
    
    df = pd.read_excel(file_path, sheet_name=target_sheet, header=None)
    logger.debug(f"DataFrame shape: {df.shape}")
    logger.debug("First few rows of DataFrame:")
    logger.debug(df.head())
    
    try:
        result = extract_section_data(df, 'I', structure, file_path, logger)
        logger.debug(f"Extracted {len(result)} rows")
        return result
    except Exception as e:
        logger.error(f"Error in extract_section_data: {str(e)}")
        logger.error(f"Structure used: {structure}")
        raise

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