import pandas as pd
import os
from pathlib import Path
from utils import (
    find_sheet_by_cell_value,
    process_multiple_files,
    setup_logger,
)

# Setup logger
logger = setup_logger('zusatzangaben')

def extract_zusatzangaben(file_path: str | Path) -> pd.DataFrame:
    """
    Extract additional information (Zusatzangaben) from the kindergarten Excel file.
    
    Args:
        file_path: Path to the Excel file to process
        
    Returns:
        pd.DataFrame: Extracted Zusatzangaben data
    """
    logger.info(f"\nProcessing file: {file_path}")
    
    # Find the correct sheet by checking content
    target_sheet = find_sheet_by_cell_value(file_path, 'ZUSATZANGABEN')
    logger.info(f"Found sheet: {target_sheet}")
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'ZUSATZANGABEN' found in {file_path}")
    
    # Read the entire sheet
    df = pd.read_excel(file_path, sheet_name=target_sheet, header=None)
    
    # Find the starting row of Zusatzangaben section
    start_row = None
    for idx, row in df.iterrows():
        if any('ZUSATZANGABEN' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            start_row = idx
            break
    
    if start_row is None:
        raise ValueError(f"Could not find 'ZUSATZANGABEN' section in {file_path}")
    
    # Initialize lists to store the data
    data = []
    
    # Process rows after the ZUSATZANGABEN header
    current_row = start_row + 2  # Skip header row
    while current_row < len(df):
        row = df.iloc[current_row]
        
        # Check if we've reached the end of the section
        if any('EINMALZAHLUNGEN' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            break
        
        # Get values from specific columns (A, C, F)
        name_eintrag = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
        eintrag = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
        erlaeuterung = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else None
        
        # Only add row if there's actual content
        if name_eintrag and name_eintrag != 'nan' and name_eintrag != '-':
            data.append({
                'Name_Eintrag': name_eintrag,
                'Eintrag': eintrag if eintrag and eintrag != 'nan' else None,
                'Erlaeuterung': erlaeuterung if erlaeuterung and erlaeuterung != 'nan' else None,
                'source_file': Path(file_path).stem
            })
        
        current_row += 1
    
    # Create DataFrame from the collected data
    result_df = pd.DataFrame(data)
    
    if len(result_df) == 0:
        raise ValueError("No Zusatzangaben data found in the file")
    
    return result_df

def process_zusatzangaben_files(
    directory_path: str | Path,
    file_pattern: str = "*.xlsx",
    checkpoint_file: str = "processed_files_zusatz.json",
    debug_limit: int | None = None
) -> pd.DataFrame:
    """
    Process multiple Excel files containing Zusatzangaben data.
    
    Args:
        directory_path: Path to directory containing Excel files
        file_pattern: Pattern to match Excel files
        checkpoint_file: Path to checkpoint file tracking processed files
        debug_limit: If set, limits the number of files to process
        
    Returns:
        pd.DataFrame: Combined data from all processed files
    """
    default_columns = ['source_file', 'Name_Eintrag', 'Eintrag', 'Erlaeuterung']
    return process_multiple_files(
        directory_path=directory_path,
        extraction_function=extract_zusatzangaben,
        file_pattern=file_pattern,
        checkpoint_file=checkpoint_file,
        debug_limit=debug_limit,
        process_type='zusatzangaben',
        default_columns=default_columns
    )

if __name__ == "__main__":
    # Get the script's directory and construct relative path
    script_dir = Path(__file__).parent
    directory_path = script_dir.parent / "02_data" / "01_input"
    checkpoint_file = directory_path.parent / "processed_files_zusatz.json"
    
    # Set debug_limit to process only a few files (set to None for processing all files)
    debug_limit = 1
    
    # Set logging level to DEBUG
    logger.setLevel('DEBUG')
    
    try:
        results = process_zusatzangaben_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        if results.empty:
            logger.warning("No data was extracted from the processed files")
            exit(1)
            
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results['source_file'].nunique()}")
        logger.info(f"Total Zusatzangaben records: {len(results)}")
        
        # Save to CSV
        output_path = directory_path.parent / "02_output" / "kindergarten_zusatzangaben.csv"
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise
