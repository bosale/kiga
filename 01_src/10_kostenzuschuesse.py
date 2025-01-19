import pandas as pd
from pathlib import Path
from utils import (
    find_sheet_with_content,
    process_multiple_files,
    setup_logger,
    extract_section_data,
    load_structure
)

# Setup logger
logger = setup_logger('kostenzuschuesse')

def extract_kostenzuschuesse(file_path: str | Path) -> pd.DataFrame:
    """
    Extract MA 10 subsidies data from the Excel file.
    
    Args:
        file_path: Path to the Excel file to process
        
    Returns:
        pd.DataFrame: Extracted subsidies data
    """
    logger.info(f"\nProcessing file: {file_path}")
    
    structure = load_structure("kostenzuschuesse_einnahmen.yaml")
    
    # Try different possible sheet identifiers
    possible_identifiers = ['REINVESTITION']
    target_sheet = "NB_KIGA"
    
    """ keep this for reference
    for identifier in possible_identifiers:
        target_sheet = find_sheet_with_content(file_path, identifier)
        if target_sheet:
            logger.info(f"Found sheet using identifier '{identifier}': {target_sheet}")
            break
    
    if target_sheet is None:
        raise ValueError(f"No suitable sheet found in {file_path}")
    """
    # Read the Excel file
    df = pd.read_excel(file_path, sheet_name=target_sheet, header=None)
    
    # Use extract_section_data to get the results
    try:
        results = extract_section_data(
            df=df,
            section_identifier="II. KOSTENZUSCHÜSSE DER MA 10",  # For "II. KOSTENZUSCHÜSSE DER MA 10"
            structure=structure,
            file_path=file_path,
            logger=logger
        )
        
        if results.empty:
            logger.warning("No data was extracted from the file")
            raise ValueError("No data extracted")
            
        logger.debug(f"Successfully extracted {len(results)} rows of data")
        return results
        
    except Exception as e:
        logger.error(f"Error extracting section data: {str(e)}")
        logger.debug("DataFrame shape at error: {}".format(df.shape))
        raise

def process_kostenzuschuesse_files(
    directory_path: str | Path,
    file_pattern: str = "*.xlsx",
    checkpoint_file: str = "processed_files_kostenzuschuesse.json",
    debug_limit: int | None = None
) -> pd.DataFrame:
    """
    Process multiple Excel files containing MA 10 subsidies data.
    
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
        extraction_function=extract_kostenzuschuesse,
        file_pattern=file_pattern,
        checkpoint_file=checkpoint_file,
        debug_limit=debug_limit,
        process_type='kostenzuschuesse',
        default_columns=default_columns
    )

if __name__ == "__main__":
    # Get the script's directory and construct relative path
    script_dir = Path(__file__).parent
    directory_path = script_dir.parent / "02_data" / "01_input"
    checkpoint_file = directory_path.parent / "processed_files_kostenzuschuesse.json"
    
    # Set debug_limit to process only a few files (set to None for processing all files)
    debug_limit = 1
    
    # Set logging level to DEBUG
    logger.setLevel('DEBUG')
    
    try:
        results = process_kostenzuschuesse_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        if results.empty:
            logger.warning("No data was extracted from the processed files")
            exit(1)
            
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results['source_file'].nunique()}")
        logger.info(f"Total subsidy records: {len(results)}")
        
        # Save to CSV
        output_path = directory_path.parent / "02_output" / "kindergarten_kostenzuschuesse.csv"
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise 