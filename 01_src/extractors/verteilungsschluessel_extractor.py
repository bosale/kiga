"""
Extractor for Verteilungsschluessel (distribution key) data from kindergarten Excel files.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, Optional, Tuple
import logging
from .base_extractor import BaseExcelExtractor

class VerteilungsschluesselExtractor(BaseExcelExtractor):
    """Extractor for Verteilungsschluessel data."""
    
    def __init__(self, config: Dict, logger: Optional[logging.Logger] = None):
        """Initialize the extractor with configuration."""
        super().__init__(config, logger)
        self.validate_config_sections(['sheet_patterns', 'section_marker', 'columns', 'headers', 'years'])
        
    def _find_data_columns(self, df: pd.DataFrame, start_row: int) -> Tuple[Optional[int], Optional[int]]:
        """Find the columns containing Kindergarten and Hort data."""
        kg_col = None
        hort_col = None
        
        # Look in the row before the first year row for the column headers
        for col in df.columns:
            if pd.notna(df.iloc[start_row-1, col]):
                header = str(df.iloc[start_row-1, col]).strip()
                if self.config['headers']['kindergarten'] in header:
                    kg_col = col
                elif self.config['headers']['hort'] in header:
                    hort_col = col
                    
        return kg_col, hort_col
    
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract Verteilungsschluessel data from Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted data with distribution keys
            
        Raises:
            ValueError: If required data cannot be found
        """
        file_path = Path(file_path)
        
        # Find the correct sheet
        xl = pd.ExcelFile(str(file_path))
        sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
        
        # Read the full sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        
        # Find the starting row of Verteilungsschluessel section
        start_row = self._find_section_start(df, self.config['section_marker'])
        if start_row is None:
            raise ValueError(f"Could not find '{self.config['section_marker']}' section")
        
        # Initialize data dictionary
        data = {'source_file': file_path.stem}
        for year in self.config['years']:
            data[f'kindergarten_{year}'] = None
            data[f'hort_{year}'] = None
        
        # Find the columns containing the percentage data
        kg_col, hort_col = self._find_data_columns(df, start_row)
        
        # Look for the data rows
        for idx in range(start_row, min(start_row + 10, len(df))):
            row = df.iloc[idx]
            
            # Look for year rows
            for col in range(len(row)):
                cell_value = str(row[col]).strip() if pd.notna(row[col]) else ''
                
                # Check for year identifiers
                if cell_value in self.config['years']:
                    if kg_col is not None:
                        data[f'kindergarten_{cell_value}'] = df.iloc[idx, kg_col]
                    if hort_col is not None:
                        data[f'hort_{cell_value}'] = df.iloc[idx, hort_col]
        
        # Convert to DataFrame
        result_df = pd.DataFrame([data])
        
        # Convert percentage values to floats
        for col in result_df.columns:
            if col != 'source_file':
                result_df[col] = pd.to_numeric(
                    result_df[col].str.rstrip('%').astype(float) / 100 
                    if isinstance(result_df[col].iloc[0], str) 
                    else result_df[col]
                )
        
        self.logger.info(f"Extracted data: {result_df.to_dict('records')[0]}")
        return result_df 