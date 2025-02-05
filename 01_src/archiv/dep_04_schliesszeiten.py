"""
Extract closing times (Schliesszeiten) from kindergarten Excel files.
"""

import os
from pathlib import Path
import yaml
import logging

from extractors.schliesszeiten_extractor import SchliesszeitenExtractor

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def main():
    """Main function to process Schliesszeiten data from Excel files."""
    logger = setup_logging()
    
    try:
        # Get the script's directory and construct paths
        script_dir = Path(__file__).parent
        config_path = script_dir / "config" / "schliesszeiten_structure.yaml"
        input_dir = script_dir.parent / "02_data" / "01_input"
        output_dir = script_dir.parent / "02_data" / "02_output"
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load configuration
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        # Initialize extractor
        extractor = SchliesszeitenExtractor(config, logger)
        
        # Process files
        results = extractor.process_files(
            directory_path=input_dir,
            file_pattern="*.xlsx",
            debug_limit=None
        )
        
        # Save results
        output_path = output_dir / "kindergarten_schliesszeiten.csv"
        results.to_csv(output_path, index=False)
        logger.info(f"Results saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main() 