import pandas as pd
import os
from pathlib import Path
from utils import (
    find_sheet_with_content,
    process_multiple_files,
    setup_logger,
    extract_section_data
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
    
    structure = load_structure()
    target_sheet = find_sheet_with_content(file_path, 'A. AUSGABEN')
    logger.info(f"Found sheet: {target_sheet}")
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'A. AUSGABEN' found in {file_path}")
    
    df = pd.read_excel(file_path, sheet_name=target_sheet, header=None)
    return extract_section_data(df, 'II', structure, file_path, logger)

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