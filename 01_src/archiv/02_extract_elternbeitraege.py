import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path
import json
from utils import find_sheet_with_content
import argparse
import logging
from extractors.elternbeitraege_extractor import ElternbeitraegeExtractor
from utils import setup_logger

def extract_elternbeitraege(file_path):
    """
    Extract parent contribution data (Elternbeiträge) from the kindergarten Excel file.
    Returns a DataFrame with the extracted data and the filename.
    """
    # Read the Excel file
    xl = pd.ExcelFile(file_path)
    print(file_path)
    
    # Find the correct sheet using helper function
    target_sheet = find_sheet_with_content(file_path, 'ELTERNBEITRÄGE')
    
    if target_sheet is None:
        raise ValueError(f"No sheet containing 'ELTERNBEITRÄGE' found in {file_path}")
    
    # Find the starting row containing "KINDERGÄRTEN UND KINDERGRUPPEN"
    preview_df = pd.read_excel(
        file_path,
        sheet_name=target_sheet,
        nrows=50  # Read more rows to ensure we find the header
    )
    
    start_row = None
    for idx, row in preview_df.iterrows():
        if any('KINDERGÄRTEN UND KINDERGRUPPEN' in str(val).upper() 
               for val in row.values 
               if pd.notna(val)):
            start_row = idx
            break
    
    if start_row is None:
        raise ValueError(f"Could not find 'KINDERGÄRTEN UND KINDERGRUPPEN' section in {file_path}")
    
    # Read the identified sheet starting from the row after the section header
    df = pd.read_excel(
        file_path,
        sheet_name=target_sheet,
        skiprows=start_row + 2,  # Skip the section header and the column headers
        nrows=30,     # Read enough rows to capture all potential entries
        usecols="A:G"  # Changed to include all relevant columns
    )
    
    # Add sanity check - sum of all numeric values in initial df
    # Fix: Only sum the 'Betrag in EUR' column
    initial_sum = pd.to_numeric(df['Betrag in EUR'], errors='coerce').sum()
    
    # Initialize lists to store the data
    data = []
    print(df.head())
    # Process Verpflegung section
    verpflegung_types = [
        'Verpflegung Halbtagsbetreuung',
        'Verpflegung Teilzeitbetreuung',
        'Verpflegung Ganztagsbetreuung'
    ]
    
    for _, row in df.iterrows():
        category = row.iloc[0]
        if pd.isna(category):
            continue
        if category.strip() in verpflegung_types:
            # Find the amount and frequency using column names
            amount = row['Betrag in EUR'] if 'Betrag in EUR' in row.index else None
            frequency = row['Anzahl pro Jahr\n(z.B. 12 mal)'] if 'Anzahl pro Jahr\n(z.B. 12 mal)' in row.index else None
            
            data.append({
                'category': 'Verpflegung',
                'type': category,
                'amount': amount if not pd.isna(amount) else None,
                'frequency': frequency if not pd.isna(frequency) else None
            })
            print(data)
    # Process Zusatzleistungen section
    zusatz_start = df[df.iloc[:, 0] == 'Zusatzleistungen'].index
    if len(zusatz_start) > 0:
        zusatz_idx = zusatz_start[0]
        for idx in range(zusatz_idx + 1, len(df)):
            row = df.iloc[idx]
            if pd.isna(row.iloc[0]):
                continue
            if row.iloc[0].startswith('Einmalzahlungen'):  # Stop when we reach Einmalzahlungen
                break
                
            data.append({
                'category': 'Zusatzleistungen',
                'type': row.iloc[0],
                'amount': row.iloc[2] if not pd.isna(row.iloc[2]) else None,
                'frequency': row.iloc[3] if not pd.isna(row.iloc[3]) else None
            })
    
    # Create DataFrame from the collected data
    result_df = pd.DataFrame(data)
    
    # Clean up the data - replace NaN with None
    result_df = result_df.replace({np.nan: None})
    
    # Add filename to the DataFrame
    result_df['source_file'] = Path(file_path).stem
    
    # Add sanity check - sum of amounts in result_df
    result_sum = pd.to_numeric(result_df['amount'], errors='coerce').sum()
    
    if initial_sum > 0 and result_sum == 0:
        warning_msg = (
            f"Source file has non-zero values (sum: {initial_sum:.2f} EUR) "
            f"but extracted results sum to zero. This might indicate data extraction issues."
        )
        print(f"\n⚠️ WARNING: {warning_msg}")
        raise ValueError(warning_msg)  # Raise error to log this in problematic_files
    
    return result_df, initial_sum, result_sum

def get_processed_files(checkpoint_file):
    """Read the checkpoint file containing already processed files"""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return set(json.load(f))
    return set()

def update_checkpoint(checkpoint_file, processed_file):
    """Update the checkpoint file with newly processed file"""
    processed_files = get_processed_files(checkpoint_file)
    processed_files.add(processed_file)
    with open(checkpoint_file, 'w') as f:
        json.dump(list(processed_files), f)

def process_multiple_files(directory_path, file_pattern="*.xlsx", checkpoint_file="processed_files.json", debug_limit=None):
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
    Extracts Elternbeiträge data from each file and logs problematic files.
    
    Args:
        directory_path (str): Path to directory containing Excel files
        file_pattern (str): Pattern to match Excel files
        checkpoint_file (str): Path to checkpoint file
        debug_limit (int, optional): If set, limits the number of files to process and ignores checkpoints
    """
    # Get list of all Excel files in the directory
    file_paths = glob.glob(os.path.join(directory_path, file_pattern))
    
    if not file_paths:
        raise FileNotFoundError(f"No Excel files found in {directory_path}")
    
    # Limit files if in debug mode
    if debug_limit is not None:
        file_paths = file_paths[:debug_limit]
        print(f"DEBUG MODE: Processing only {debug_limit} files (ignoring checkpoints)")
        processed_files = set()  # Empty set in debug mode
    else:
        # Get already processed files
        processed_files = get_processed_files(checkpoint_file)
    
    # Initialize list for results, problematic files, and sums
    all_results = []
    problematic_files = []
    total_initial_sum = 0
    total_result_sum = 0
    
    # Process each file
    for file_path in file_paths:
        file_name = Path(file_path).name
        if debug_limit is not None or file_name not in processed_files:
            try:
                # Extract Elternbeiträge
                df_beitraege, initial_sum, result_sum = extract_elternbeitraege(file_path)
                all_results.append(df_beitraege)
                total_initial_sum += initial_sum
                total_result_sum += result_sum
                
                print(f"Successfully processed file: {file_name}")
                if debug_limit is None:  # Only update checkpoint if not in debug mode
                    update_checkpoint(checkpoint_file, file_name)
            except Exception as e:
                error_message = str(e)
                error_type = type(e).__name__
                print(f"Error processing {file_name}: {error_message}")
                problematic_files.append({
                    'file_name': file_name,
                    'error_type': error_type,
                    'error_description': error_message,
                    'initial_sum': initial_sum if 'initial_sum' in locals() else None,
                    'result_sum': result_sum if 'result_sum' in locals() else None,
                    'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                continue
    
    # Save problematic files to CSV if any exist
    if problematic_files:
        problems_df = pd.DataFrame(problematic_files)
        problems_path = os.path.join(os.path.dirname(directory_path), "problematic_files.csv")
        # Sort by timestamp and ensure consistent column order
        problems_df = problems_df[[
            'file_name', 'error_type', 'error_description', 
            'initial_sum', 'result_sum', 'timestamp'
        ]].sort_values('timestamp', ascending=False)
        problems_df.to_csv(problems_path, index=False)
        print(f"\nProblematic files logged to: {problems_path}")
        print(f"Number of problematic files: {len(problematic_files)}")
    
    # Combine results
    if not all_results:
        raise ValueError("No files were successfully processed")
    
    combined_df = pd.concat(all_results, ignore_index=True)
    
    print("\nSanity Check Summary:")
    print(f"Total sum in source files: {total_initial_sum:.2f}")
    print(f"Total sum in extracted results: {total_result_sum:.2f}")
    if total_initial_sum > 0 and total_result_sum == 0:
        print("⚠️ WARNING: Source files contain non-zero values but extracted results sum to zero!")
    
    return combined_df

def clear_checkpoints(checkpoint_file="processed_files.json"):
    """Clear the checkpoint file to start fresh"""
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        print("Checkpoint file cleared.")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Extract parent contribution data from Excel files.')
    parser.add_argument(
        '--input-dir',
        type=str,
        help='Directory containing input Excel files'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Directory for output files'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to extractor configuration file'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode (process only one file)'
    )
    return parser.parse_args()

def get_default_paths() -> dict:
    """Get default paths relative to script location."""
    script_dir = Path(__file__).parent
    return {
        'input_dir': script_dir.parent / "02_data" / "01_input",
        'output_dir': script_dir.parent / "02_data" / "02_output",
        'config': script_dir / "config" / "elternbeitraege_structure.yaml"
    }

def validate_paths(paths: dict) -> None:
    """
    Validate existence of required paths and create output directory if needed.
    
    Args:
        paths: Dictionary containing path configurations
        
    Raises:
        FileNotFoundError: If required input paths don't exist
    """
    if not paths['input_dir'].exists():
        raise FileNotFoundError(f"Input directory not found: {paths['input_dir']}")
        
    if not paths['config'].exists():
        raise FileNotFoundError(f"Configuration file not found: {paths['config']}")
        
    # Create output directory if it doesn't exist
    paths['output_dir'].mkdir(parents=True, exist_ok=True)

def main():
    # Setup logger
    logger = setup_logger('elternbeitraege')
    
    try:
        # Parse arguments and get paths
        args = parse_args()
        default_paths = get_default_paths()
        
        paths = {
            'input_dir': Path(args.input_dir) if args.input_dir else default_paths['input_dir'],
            'output_dir': Path(args.output_dir) if args.output_dir else default_paths['output_dir'],
            'config': Path(args.config) if args.config else default_paths['config']
        }
        
        # Validate paths
        validate_paths(paths)
        
        # Check for input files
        excel_files = list(paths['input_dir'].glob("*.xlsx"))
        logger.info(f"Found {len(excel_files)} Excel files in {paths['input_dir']}")
        for file in excel_files:
            logger.info(f"  - {file.name}")
        
        if not excel_files:
            raise FileNotFoundError(f"No Excel files found in {paths['input_dir']}")
        
        # Set debug mode
        debug_limit = 1 if args.debug else None
        if debug_limit:
            logger.info("Running in debug mode - will process only one file")
        
        # Initialize extractor
        extractor = ElternbeitraegeExtractor(paths['config'])
        
        # Process files
        results_df = extractor.process_files(
            directory_path=paths['input_dir'],
            debug_limit=debug_limit
        )
        
        # Print summary
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results_df['source_file'].nunique()}")
        logger.info(f"Total records: {len(results_df)}")
        
        # Save to CSV
        output_path = paths['output_dir'] / "kindergarten_elternbeitraege.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results_df.head())
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 