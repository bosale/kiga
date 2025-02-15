"""
Main script for extracting data from kindergarten Excel files.
Supports multiple types of data extraction.
"""

import argparse
from pathlib import Path
import logging
import yaml
from extractors.kindergarten_extractor import KindergartenExcelExtractor
from extractors.elternbeitraege_extractor import ElternbeitraegeExtractor
from extractors.zusatzangaben_extractor import ZusatzangabenExtractor
from extractors.schliesszeiten_extractor import SchliesszeitenExtractor
from extractors.oeffnungszeiten_extractor import OeffnungszeitenExtractor
from extractors.verpflegung_extractor import VerpflegungExtractor
from extractors.anlagenverzeichnis_extractor import AnlagenverzeichnisExtractor
from extractors.verteilungsschluessel_extractor import VerteilungsschluesselExtractor
from extractors.personalausgaben_extractor import PersonalausgabenExtractor
from extractors.sachausgaben_extractor import SachausgabenExtractor
from extractors.einnahmen_extractor import EinnahmenExtractor
from extractors.vermoegen_extractor import VermoegenExtractor
from extractors.verbindlichkeiten_extractor import VerbindlichkeitenExtractor
from utils import setup_logger
from utils.db_utils import write_to_sql
import sql_data_types

# Map of extraction types to their respective extractor classes
# Optional SQL types can be provided to override automatic type inference
EXTRACTORS = {
    'deckblatt': {'class': KindergartenExcelExtractor, 'sql_types': None},
    'elternbeitraege': {'class': ElternbeitraegeExtractor, 'sql_types': None},
    'zusatzangaben': {'class': ZusatzangabenExtractor, 'sql_types': None},
    'schliesszeiten': {'class': SchliesszeitenExtractor, 'sql_types': None},
    'oeffnungszeiten': {'class': OeffnungszeitenExtractor, 'sql_types': None},
    'verpflegung': {'class': VerpflegungExtractor, 'sql_types': None},
    'anlagenverzeichnis': {'class': AnlagenverzeichnisExtractor, 'sql_types': None},
    'verteilungsschluessel': {'class': VerteilungsschluesselExtractor, 'sql_types': sql_data_types.sql_types_verteilungsschluessel},
    'personalausgaben': {'class': PersonalausgabenExtractor, 'sql_types': None},
    'sachausgaben': {'class': SachausgabenExtractor, 'sql_types': None},
    'einnahmen': {'class': EinnahmenExtractor, 'sql_types': None},
    'vermoegen': {'class': VermoegenExtractor, 'sql_types': None},
    'verbindlichkeiten': {'class': VerbindlichkeitenExtractor, 'sql_types': None}
}

# Special config file names that don't follow the standard pattern
CONFIG_FILE_NAMES = {
    'vermoegen': 'vermoegensuebersicht_structure.yaml'
}

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Extract data from kindergarten Excel files.')
    parser.add_argument(
        '--type',
        type=str,
        choices=list(EXTRACTORS.keys()),
        required=True,
        help='Type of data to extract'
    )
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
    parser.add_argument(
        '--no-sql',
        action='store_true',
        help='Skip writing to SQL Server'
    )
    return parser.parse_args()

def get_default_paths(extraction_type: str) -> dict:
    """Get default paths relative to script location."""
    script_dir = Path(__file__).parent
    
    # Get the correct config file name
    config_file = CONFIG_FILE_NAMES.get(extraction_type, f"{extraction_type}_structure.yaml")
    
    return {
        'input_dir': script_dir.parent / "02_data" / "01_input",
        'output_dir': script_dir.parent / "02_data" / "02_output",
        'config': script_dir / "config" / config_file
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

def load_config(config_path: Path) -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        dict: Configuration dictionary
        
    Raises:
        ValueError: If config file is invalid
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        if not isinstance(config, dict):
            raise ValueError(f"Invalid config format in {config_path}")
        return config
    except Exception as e:
        raise ValueError(f"Error loading config from {config_path}: {str(e)}")

def main():
    # Parse arguments
    args = parse_args()
    
    # Setup logger for the specific extraction type
    logger = setup_logger(args.type)
    
    try:
        # Get default paths for the specific extraction type
        default_paths = get_default_paths(args.type)
        
        paths = {
            'input_dir': Path(args.input_dir) if args.input_dir else default_paths['input_dir'],
            'output_dir': Path(args.output_dir) if args.output_dir else default_paths['output_dir'],
            'config': Path(args.config) if args.config else default_paths['config']
        }
        
        # Validate paths
        validate_paths(paths)
        
        # Load configuration
        config = load_config(paths['config'])
        
        # Initialize the appropriate extractor
        extractor_info = EXTRACTORS[args.type]
        extractor = extractor_info['class'](config)
        
        # Process files
        results_df = extractor.process_files(
            directory_path=paths['input_dir'],
            debug_limit=1 if args.debug else None
        )
        
        # Save results to CSV
        output_file = f"kindergarten_{args.type}.csv"
        output_path = paths['output_dir'] / output_file
        results_df.to_csv(output_path, index=False)
        
        # Write to SQL Server if not disabled
        if not args.no_sql:
            try:
                write_to_sql(
                    df=results_df,
                    table_name=f"kindergarten_{args.type}",
                    sql_types=extractor_info['sql_types'],
                    logger=logger
                )
            except Exception as e:
                logger.error(f"Failed to write to SQL Server: {str(e)}")
                logger.error("Data was saved to CSV but not to SQL Server")
        
        # Print summary
        logger.info("\nExtraction Summary:")
        logger.info(f"Type: {args.type}")
        logger.info(f"Total files processed: {results_df['source_file'].nunique()}")
        logger.info(f"Total records: {len(results_df)}")
        logger.info(f"\nResults saved to: {output_path}")
        if not args.no_sql:
            logger.info(f"Results also written to SQL Server table: kindergarten_{args.type}")
        logger.info("\nSample of extracted data:")
        logger.info(results_df.head())
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 