import pandas as pd
import yaml
from pathlib import Path
from typing import Tuple
from utils import (
    find_sheet_by_cell_value,
    process_multiple_files,
    setup_logger,
)

# Setup logger
logger = setup_logger('verpflegung')

def load_structure() -> list:
    """Load the expected structure from YAML file."""
    config_path = Path(__file__).parent / "config" / "verpflegung_structure.yaml"
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

def find_years(df: pd.DataFrame, start_row: int) -> Tuple[str, str]:
    """Find the two years in the verpflegung section."""
    # Look for years in the rows after the header
    for idx in range(start_row, start_row + 10):  # Increased search range
        row = df.iloc[idx]
        # Print for debugging
        logger.debug(f"Checking row {idx}: {row.values}")
        
        # First try direct numeric values
        years = [str(val).strip() for val in row if pd.notna(val) and str(val).strip().isdigit()]
        
        # If not found, try to extract years from cells that might contain other text
        if len(years) != 2:
            years = []
            for val in row:
                if pd.notna(val):
                    val_str = str(val).strip()
                    # Look for 4-digit numbers that could be years
                    import re
                    found_years = re.findall(r'\b20\d{2}\b', val_str)
                    years.extend(found_years)
        
        if len(years) == 2:
            logger.debug(f"Found years: {years}")
            return years[0], years[1]
            
    raise ValueError(f"Could not find years in the expected range. Searched rows {start_row} to {start_row + 10}")

def extract_verpflegung(file_path: str | Path) -> pd.DataFrame:
    """
    Extract Verpflegung information from the kindergarten Excel file.
    
    Args:
        file_path: Path to the Excel file to process
        
    Returns:
        pd.DataFrame: Extracted Verpflegung data
    """
    logger.info(f"\nProcessing file: {file_path}")
    
    # Find the correct sheet
    target_sheet = find_sheet_by_cell_value(file_path, 'NB_VERPFLEGUNG')
    logger.info(f"Found sheet: {target_sheet}")
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'NB_VERPFLEGUNG' found in {file_path}")
    
    # Read the entire sheet
    df = pd.read_excel(file_path, sheet_name=target_sheet, header=None)
    # save to csv
    df.to_csv(f"debug_verpflegung.csv", index=False)
    # Load expected structure
    structure = load_structure()
    
    # Find the starting point of the data
    start_row = None
    for idx, row in df.iterrows():
        if any('NB_VERPFLEGUNG' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            start_row = idx
            break
    
    if start_row is None:
        raise ValueError(f"Could not find 'NB_VERPFLEGUNG' section in {file_path}")
    
    # Get the years
    year_x, year_y = find_years(df, start_row)
    
    # Initialize data storage
    data = []
    
    # Process each category from the structure
    current_row = start_row
    while current_row < len(df):
        row = df.iloc[current_row]
        
        # Check each cell in the row for matching category
        for cell in row:
            if pd.notna(cell):
                cell_str = str(cell).strip()
                # Check if this cell contains any of our structure items
                for category in structure:
                    if category.lower() in cell_str.lower():
                        # Get the values for both years (assuming they're in the next columns)
                        values_row = df.iloc[current_row]
                        
                        # Special handling for Selbstkocher which has Ja/Nein values
                        if "Selbstkocher" in category:
                            # Find the "Ja" or "Nein" values
                            values = [str(val).strip() for val in values_row if pd.notna(val) and 
                                    str(val).strip().lower() in ['ja', 'nein']]
                            if len(values) >= 2:
                                year_x_val = values[0]
                                year_y_val = values[1]
                            else:
                                continue
                        else:
                            # Original numeric value handling
                            values = [val for val in values_row if pd.notna(val) and 
                                    (isinstance(val, (int, float)) or 
                                     (isinstance(val, str) and any(c.isdigit() for c in val)))]
                            
                            # Clean and convert values
                            year_x_val = None
                            year_y_val = None
                            
                            for val in values:
                                if isinstance(val, str):
                                    # Handle percentage values
                                    if '%' in val:
                                        val = val.replace('%', '').strip()
                                    # Handle currency values
                                    val = str(val).replace('€', '').replace(',', '').strip()
                                
                                try:
                                    val = float(val)
                                    if year_x_val is None:
                                        year_x_val = val
                                    else:
                                        year_y_val = val
                                        break
                                except (ValueError, TypeError):
                                    continue
                        
                        if year_x_val is not None or year_y_val is not None:
                            data.append({
                                'category': category,
                                f'year_{year_x}': year_x_val,
                                f'year_{year_y}': year_y_val,
                                'source_file': Path(file_path).stem
                            })
                        
        current_row += 1
    
    # Create DataFrame from the collected data
    result_df = pd.DataFrame(data)
    
    if len(result_df) == 0:
        raise ValueError("No Verpflegung data found in the file")
    
    return result_df

def process_verpflegung_files(
    directory_path: str | Path,
    file_pattern: str = "*.xlsx",
    checkpoint_file: str = "processed_files_verpflegung.json",
    debug_limit: int | None = None
) -> pd.DataFrame:
    """
    Process multiple Excel files containing Verpflegung data.
    
    Args:
        directory_path: Path to directory containing Excel files
        file_pattern: Pattern to match Excel files
        checkpoint_file: Path to checkpoint file tracking processed files
        debug_limit: If set, limits the number of files to process
        
    Returns:
        pd.DataFrame: Combined data from all processed files
    """
    return process_multiple_files(
        directory_path=directory_path,
        extraction_function=extract_verpflegung,
        file_pattern=file_pattern,
        checkpoint_file=checkpoint_file,
        debug_limit=debug_limit,
        process_type='verpflegung'
    )

if __name__ == "__main__":
    # Get the script's directory and construct relative path
    script_dir = Path(__file__).parent
    directory_path = script_dir.parent / "02_data" / "01_input"
    checkpoint_file = directory_path.parent / "processed_files_verpflegung.json"
    
    # Set debug_limit to process only a few files (set to None for processing all files)
    debug_limit = 1
    
    # Set logging level to DEBUG
    logger.setLevel('DEBUG')
    
    try:
        results = process_verpflegung_files(
            directory_path, 
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        if results.empty:
            logger.warning("No data was extracted from the processed files")
            exit(1)
            
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results['source_file'].nunique()}")
        logger.info(f"Total Verpflegung records: {len(results)}")
        
        # Save to CSV
        output_path = directory_path.parent / "02_output" / "kindergarten_verpflegung.csv"
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise 