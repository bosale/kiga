"""
Extractor for Anlagenverzeichnis (asset register) data from Excel files.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional
import logging

from .base_extractor import BaseExcelExtractor

class AnlagenverzeichnisExtractor(BaseExcelExtractor):
    """Extracts asset register data from Excel files."""
    
    def __init__(self, config: Dict, logger: Optional[logging.Logger] = None):
        """Initialize the Anlagenverzeichnis extractor."""
        super().__init__(config, logger)
        self.validate_config_sections(['sheet_patterns', 'header_marker', 'columns', 'exclude_patterns'])
        
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract asset register data from an Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            pd.DataFrame: Extracted asset register data
        """
        self.logger.info(f"\nProcessing file: {file_path}")
        
        # Find the correct sheet
        xl = pd.ExcelFile(str(file_path))
        sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
        
        # Read the Excel file
        df = pd.read_excel(str(file_path), sheet_name=sheet_name, header=None)
        self.logger.info(f"📊 Successfully read '{sheet_name}' sheet")
        
        # Find the header row using the marker
        header_mask = df.apply(lambda x: x.astype(str).str.contains(self.config['header_marker'], case=False, na=False))
        header_rows = header_mask.any(axis=1)
        if not header_rows.any():
            raise ValueError(f"Could not find header row with '{self.config['header_marker']}'")
        
        header_row = header_rows.idxmax()
        self.logger.info(f"📋 Found header row at index {header_row}")
        
        # Set the header row and skip to the data
        df.columns = df.iloc[header_row]
        df = df.iloc[header_row + 1:].reset_index(drop=True)
        
        # Clean column names and rename according to config
        df.columns = df.columns.str.replace('\n', ' ').str.strip()
        
        # Create column mapping from config
        column_mapping = {
            col['original_name']: col['name']
            for col in self.config['columns']
        }
        
        # Rename the columns
        df = df.rename(columns=column_mapping)
        
        self.logger.info(f"Found columns: {', '.join(df.columns)}")
        
        # Convert columns according to their types
        for col_config in self.config['columns']:
            col_name = col_config['name']
            col_type = col_config['type']
            
            if col_type == 'float':
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            elif col_type == 'date':
                df[col_name] = pd.to_datetime(
                    df[col_name], 
                    format=col_config.get('format', '%d.%m.%Y'), 
                    errors='coerce'
                )
        
        # Add source file column
        df['source_file'] = str(file_path)
        
        # Remove empty rows
        df = df.dropna(subset=[self.config['columns'][0]['name']], how='all')
        
        # Remove summary rows based on exclude patterns
        for pattern in self.config['exclude_patterns']:
            df = df[~df[self.config['columns'][0]['name']].str.contains(pattern, case=False, na=False)]
        
        return df 