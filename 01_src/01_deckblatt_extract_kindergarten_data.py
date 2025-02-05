"""
Main script for extracting kindergarten data from Excel files.
"""

import glob
from pathlib import Path
from typing import Optional
import pandas as pd

from extractors.data_extractor import KindergartenDataExtractor
from utils.checkpoint_manager.checkpoint_handler import CheckpointManager
from utils import setup_logger

# Setup logger
logger = setup_logger('deckblatt')

def process_multiple_files(
    directory_path: str | Path,
    file_pattern: str = "*.xlsx",
    checkpoint_file: Optional[str | Path] = "processed_files.json",
    debug_limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
    
    Args:
        directory_path: Path to directory containing Excel files
        file_pattern: Pattern to match Excel files
        checkpoint_file: Path to checkpoint file
        debug_limit: If set, limits the number of files to process
    
    Returns:
        pd.DataFrame: Combined results from all processed files
    """
    # Convert to Path object if string
    directory_path = Path(directory_path)
    if checkpoint_file:
        checkpoint_file = Path(checkpoint_file)
    
    # Get list of all Excel files in the directory
    file_paths = list(directory_path.glob(file_pattern))
    
    if not file_paths:
        logger.error(f"No Excel files found in {directory_path}")
        raise FileNotFoundError(f"No Excel files found in {directory_path}")
    
    logger.info(f"Found {len(file_paths)} Excel files: {[f.name for f in file_paths]}")
    
    # Limit files if in debug mode
    if debug_limit is not None:
        file_paths = file_paths[:debug_limit]
        logger.info(f"DEBUG MODE: Processing only {debug_limit} files")
        processed_files = set()  # Empty set in debug mode
    else:
        # Initialize checkpoint manager if checkpoint file is provided
        checkpoint_mgr = CheckpointManager(checkpoint_file) if checkpoint_file else None
        processed_files = checkpoint_mgr.get_processed_files() if checkpoint_mgr else set()
    
    # Process each file
    all_results = []
    problematic_files = []
    
    for file_path in file_paths:
        if debug_limit is not None or file_path.name not in processed_files:
            try:
                logger.info(f"Processing file: {file_path.name}")
                
                # Extract data from both sections
                extractor = KindergartenDataExtractor(str(file_path))
                combined_df = extractor.extract_all_sections()
                all_results.append(combined_df)
                
                logger.info(f"Successfully processed file: {file_path.name}")
                if checkpoint_mgr and not debug_limit:
                    checkpoint_mgr.update_checkpoint(file_path.name)
                    
            except Exception as e:
                error_message = str(e)
                logger.error(f"Error processing {file_path.name}: {error_message}")
                problematic_files.append({
                    'file_name': file_path.name,
                    'error_type': type(e).__name__,
                    'error_description': error_message
                })
                continue
    
    # Save problematic files to CSV if any exist
    if problematic_files:
        problems_df = pd.DataFrame(problematic_files)
        problems_path = directory_path.parent / "problematic_files_deckblatt.csv"
        problems_df.to_csv(problems_path, index=False)
        logger.warning(f"Problematic files logged to: {problems_path}")
        logger.warning(f"Number of problematic files: {len(problematic_files)}")
    
    # Combine all results
    if not all_results:
        logger.error("No files were successfully processed")
        raise ValueError("No files were successfully processed")
    
    return pd.concat(all_results, ignore_index=True)

if __name__ == "__main__":
    # Get the script's directory and construct relative paths
    script_dir = Path(__file__).parent
    directory_path = script_dir.parent / "02_data" / "01_input"
    checkpoint_file = directory_path.parent / "processed_files_deckblatt.json"
    
    # Set debug mode
    debug_limit = 1  # Set to None for processing all files
    logger.setLevel('DEBUG')
    
    try:
        # Process files
        combined_results = process_multiple_files(
            directory_path=directory_path,
            checkpoint_file=checkpoint_file,
            debug_limit=debug_limit
        )
        
        # Print summary
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {combined_results['source_file'].nunique()}")
        logger.info(f"Total records: {len(combined_results)}")
        
        # Save to CSV
        output_path = directory_path.parent / "02_output" / "kindergarten_deckblatt.csv"
        combined_results.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(combined_results.head())
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise 