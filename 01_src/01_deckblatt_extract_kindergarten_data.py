"""
Main script for extracting kindergarten data from Excel files.
"""

from pathlib import Path
import logging
from extractors.kindergarten_extractor import KindergartenExcelExtractor
from utils import setup_logger

# Setup logger
logger = setup_logger('deckblatt')

def main():
    # Get the script's directory and construct paths
    script_dir = Path(__file__).parent
    input_dir = script_dir.parent / "02_data" / "01_input"
    output_dir = script_dir.parent / "02_data" / "02_output"
    config_path = script_dir / "config" / "kindergarten_extractor_config.yaml"
    
    # Check if input directory exists and contains files
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    
    excel_files = list(input_dir.glob("*.xlsx"))
    logger.info(f"Found {len(excel_files)} Excel files in {input_dir}")
    for file in excel_files:
        logger.info(f"  - {file.name}")
    
    if not excel_files:
        logger.error(f"No Excel files found in {input_dir}")
        raise FileNotFoundError(f"No Excel files found in {input_dir}")
    
    # Set debug mode
    debug_limit = 1  # Set to None for processing all files
    
    try:
        # Initialize extractor
        extractor = KindergartenExcelExtractor(config_path)
        
        # Process files
        results_df = extractor.process_files(
            directory_path=input_dir,
            debug_limit=debug_limit
        )
        
        # Print summary
        logger.info("\nExtracted Data Summary:")
        logger.info(f"Total files processed: {results_df['source_file'].nunique()}")
        logger.info(f"Total records: {len(results_df)}")
        
        # Save to CSV
        output_path = output_dir / "kindergarten_deckblatt.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results_df.head())
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 