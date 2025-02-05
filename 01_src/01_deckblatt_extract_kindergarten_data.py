"""
Main script for extracting kindergarten data from Excel files.
"""

import argparse
from pathlib import Path
import logging
import yaml
from extractors.kindergarten_extractor import KindergartenExcelExtractor
from utils import setup_logger

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Extract kindergarten data from Excel files.')
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
        'config': script_dir / "config" / "kindergarten_extractor_config.yaml"
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
    logger = setup_logger('deckblatt')
    
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
        extractor = KindergartenExcelExtractor(paths['config'])
        
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
        output_path = paths['output_dir'] / "kindergarten_deckblatt.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"\nResults saved to: {output_path}")
        logger.info("\nSample of extracted data:")
        logger.info(results_df.head())
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 