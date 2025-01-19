import pandas as pd
from pathlib import Path
from utils import (
    process_multiple_files,
    setup_logger,
    load_structure,
)
from utils.excel_utils import extract_balance_data
    
# Setup logger
logger = setup_logger('vermoegen')

def extract_vermoegen(file_path: str | Path) -> pd.DataFrame:
    """
    Extract assets (Vermögen) data from the Excel file.
    
    Args:
        file_path: Path to the Excel file to process
        
    Returns:
        pd.DataFrame: Extracted assets data
    """
    logger.info(f"\nProcessing file: {file_path}")
    
    structure = load_structure("vermoegensuebersicht_vermoegen_structure.yaml")
    
    # Find the correct sheet
    target_sheet = "NB_Vermögensübersicht" # find_sheet_with_content(file_path, "Vermögensübersicht")
    
    if target_sheet is None:
        raise ValueError(f"⚠️ No suitable sheet found in {file_path}")
    
    df = pd.read_excel(file_path, sheet_name=target_sheet, header=None)
    
    return extract_balance_data(df, "Vermögen", structure, file_path, logger)

def process_vermoegen_files(
    directory_path: str | Path,
    file_pattern: str = "*.xlsx",
    checkpoint_file: str = "processed_files_vermoegen.json",
    debug_limit: int | None = None
) -> pd.DataFrame:
    """
    Process multiple Excel files containing assets data.
    """
    default_columns = [
        'source_file', 'category', 'item', 
        'value_2023_start', 'value_2023_end', 'change'
    ]
    return process_multiple_files(
        directory_path=directory_path,
        extraction_function=extract_vermoegen,
        file_pattern=file_pattern,
        checkpoint_file=checkpoint_file,
        debug_limit=debug_limit,
        process_type='vermoegen',
        default_columns=default_columns
    )

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    directory_path = script_dir.parent / "02_data" / "01_input"
    checkpoint_file = directory_path.parent / "processed_files_vermoegen.json"
    
    debug_limit = 1
    logger.setLevel('DEBUG')
    
    try:
        results = process_vermoegen_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        if results.empty:
            logger.warning("No data was extracted from the processed files")
            exit(1)
            
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results['source_file'].nunique()}")
        logger.info(f"Total asset records: {len(results)}")
        
        output_path = directory_path.parent / "02_output" / "vermoegen.csv"
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise 