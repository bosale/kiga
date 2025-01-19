import pandas as pd
from pathlib import Path
from typing import Tuple
from utils import (
    process_multiple_files,
    setup_logger
)

# Setup logger
logger = setup_logger('anlagenverzeichnis')

def find_header_row(df: pd.DataFrame) -> int:
    """
    Find the row containing the header 'Inventarbezeichnung'.
    
    Args:
        df: DataFrame to search in
        
    Returns:
        int: Index of the header row
    """
    header_mask = df.apply(lambda x: x.astype(str).str.contains('Inventarbezeichnung', case=False, na=False))
    header_rows = header_mask.any(axis=1)
    if not header_rows.any():
        raise ValueError("Could not find header row with 'Inventarbezeichnung'")
    else:
        logger.info(f"📋 Found header row with 'Inventarbezeichnung' at index {header_rows.idxmax()}")
        return header_rows.idxmax()

def extract_anlagenverzeichnis(file_path: str | Path) -> pd.DataFrame:
    """
    Extract asset register (Anlagenverzeichnis) data from the Excel file.
    
    Args:
        file_path: Path to the Excel file to process
        
    Returns:
        pd.DataFrame: Extracted asset register data
    """
    logger.info(f"\nProcessing file: {file_path}")
    
    # Try to read the Excel file
    try:
        # Try to read the specific sheet
        df = pd.read_excel(file_path, sheet_name="NB_Anlagenverzeichnis", header=None)
        logger.info(f"📊 Successfully read 'NB_Anlagenverzeichnis' sheet from {file_path}")
    except ValueError as e:
        # If sheet not found, log available sheets
        xl = pd.ExcelFile(file_path)
        logger.error(f"⚠️ Could not find sheet 'NB_Anlagenverzeichnis' in {file_path}")
        logger.error(f"Available sheets: {xl.sheet_names}")
        raise
    except Exception as e:
        logger.error(f"⚠️ Failed to read Excel file {file_path}: {str(e)}")
        raise

    # Find the header row
    header_row = find_header_row(df)
    
    # Set the header row and skip to the data
    df.columns = df.iloc[header_row]
    # Clean column names by replacing linebreaks with spaces and stripping whitespace
    df.columns = df.columns.str.replace('\n', ' ').str.strip()
    
    # Create a mapping for the columns with footnotes
    column_mapping = {
        'Inventarbezeichnung 1)': 'Inventarbezeichnung',
        'Lieferant 2)': 'Lieferant',
        'Anschaffung  (Datum) 3)': 'Anschaffung (Datum)',
        'Anschaffungswert 4)': 'Anschaffungswert',
        'Nutzungsdauer (Jahre) 5)': 'Nutzungsdauer (Jahre)',
        'kumulierte Abschreibung  bis 31.12.2022 6)': 'kumulierte Abschreibung bis 31.12.2022',
        'Buchwert 31.12.2022 7)': 'Buchwert 31.12.2022',
        'Abschreibung  2023 8)': 'Abschreibung 2023',
        'Buchwert 31.12.2023 9)': 'Buchwert 31.12.2023'
    }
    
    # Rename the columns
    df = df.rename(columns=column_mapping)
    df = df.iloc[header_row + 1:].reset_index(drop=True)
    
    logger.info(f"Found columns: {', '.join(df.columns)}")
    
    # Convert numeric columns
    numeric_columns = [
        'Anschaffungswert', 'Nutzungsdauer (Jahre)', 
        'kumulierte Abschreibung bis 31.12.2022', 'Buchwert 31.12.2022',
        'Abschreibung 2023', 'Buchwert 31.12.2023'
    ]
    
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert date column
    df['Anschaffung (Datum)'] = pd.to_datetime(df['Anschaffung (Datum)'], format='%d.%m.%Y', errors='coerce')
    
    # Add source file column
    df['source_file'] = str(file_path)
    
    # Remove empty rows
    df = df.dropna(subset=['Inventarbezeichnung'], how='all')
    
    # Remove summary rows (typically containing 'GESAMT')
    df = df[~df['Inventarbezeichnung'].str.contains('GESAMT', case=False, na=False)]
    
    return df

def process_anlagenverzeichnis_files(
    directory_path: str | Path,
    file_pattern: str = "*.xlsx",
    checkpoint_file: str = "processed_files_anlagenverzeichnis.json",
    debug_limit: int | None = None
) -> pd.DataFrame:
    """
    Process multiple Excel files containing asset register data.
    """
    default_columns = [
        'source_file', 'Inventarbezeichnung', 'Lieferant', 
        'Anschaffung (Datum)', 'Anschaffungswert', 'Nutzungsdauer (Jahre)',
        'kumulierte Abschreibung bis 31.12.2022', 'Buchwert 31.12.2022',
        'Abschreibung 2023', 'Buchwert 31.12.2023'
    ]
    
    return process_multiple_files(
        directory_path=directory_path,
        extraction_function=extract_anlagenverzeichnis,
        file_pattern=file_pattern,
        checkpoint_file=checkpoint_file,
        debug_limit=debug_limit,
        process_type='anlagenverzeichnis',
        default_columns=default_columns
    )

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    directory_path = script_dir.parent / "02_data" / "01_input"
    checkpoint_file = directory_path.parent / "processed_files_anlagenverzeichnis.json"
    
    logger.info(f"Looking for Excel files in: {directory_path}")
    excel_files = list(directory_path.glob("*.xlsx"))
    logger.info(f"Found {len(excel_files)} Excel files: {[f.name for f in excel_files]}")
    
    debug_limit = 1
    logger.setLevel('DEBUG')
    
    try:
        results = process_anlagenverzeichnis_files(
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
        
        output_path = directory_path.parent / "02_output" / "anlagenverzeichnis.csv"
        results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results.head())
        
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise 