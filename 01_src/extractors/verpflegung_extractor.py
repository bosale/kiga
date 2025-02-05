"""
Concrete implementation of Excel extractor for Verpflegung (catering) data.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional, Tuple
import re
import logging
import sys

from .base_extractor import BaseExcelExtractor

class VerpflegungExtractor(BaseExcelExtractor):
    """Extractor for Verpflegung (catering) data from Excel files."""
    
    def __init__(self, config: Dict, logger: Optional[logging.Logger] = None):
        """Initialize the extractor with configuration."""
        super().__init__(config, logger)
        self.validate_config_sections(['verpflegung_rows', 'boolean_fields', 'numeric_fields'])
        
        # Only add console handler if no handlers exist
        if not self.logger.handlers:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            self.logger.setLevel(logging.INFO)
        
        self.logger.info("VerpflegungExtractor initialized")

    def _find_years(self, df: pd.DataFrame, start_row: int) -> Optional[Tuple[str, str]]:
        """
        Find the two years in the Verpflegung section.
        
        Args:
            df: DataFrame containing the data
            start_row: Row where the section starts
            
        Returns:
            Optional[Tuple] containing the two years found, or None if years not found
        """
        self.logger.info(f"Starting year search from row {start_row}")
        
        # Save first few rows to CSV for debugging
        debug_df = df.iloc[start_row:start_row+10]
        debug_path = Path('debug_verpflegung_rows.csv')
        debug_df.to_csv(debug_path, encoding='utf-8')
        self.logger.info(f"Saved first 10 rows to {debug_path.absolute()}")
        
        # Look for years in the rows after the header
        for idx in range(start_row, min(start_row + 10, len(df))):
            row = df.iloc[idx]
            row_values = [str(val) for val in row.values if pd.notna(val)]
            self.logger.info(f"Row {idx} contents: {row_values}")
            
            # First try to find a row that has exactly two 4-digit numbers
            years = []
            for val in row:
                if pd.notna(val):
                    val_str = str(val).strip()
                    self.logger.info(f"Checking value: '{val_str}'")
                    # Look for 4-digit numbers that could be years
                    found_years = re.findall(r'\b20\d{2}\b', val_str)
                    if found_years:
                        self.logger.info(f"Found potential year(s) in value '{val_str}': {found_years}")
                        years.extend(found_years)
            
            # If we found exactly two years in this row, use them
            if len(years) == 2:
                self.logger.info(f"Found years in row {idx}: {years}")
                return years[0], years[1]
            elif len(years) > 0:
                self.logger.info(f"Found {len(years)} years in row {idx}, need exactly 2: {years}")
        
        self.logger.warning(f"Could not find exactly two years in the expected range. Searched rows {start_row} to {min(start_row + 10, len(df) - 1)}")
        
        # Additional debug info
        self.logger.info("DataFrame info:")
        self.logger.info(f"Total rows: {len(df)}")
        self.logger.info(f"Columns: {df.columns.tolist()}")
        self.logger.info("First few rows of data:")
        for idx in range(start_row, min(start_row + 5, len(df))):
            self.logger.info(f"Row {idx}: {[str(x) for x in df.iloc[idx].tolist()]}")
        
        return None

    def _extract_value(self, row: pd.Series, field: str, category_col: int) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract numeric values from fixed column offsets relative to category column.
        
        Args:
            row: DataFrame row to extract values from
            field: Field name being processed
            category_col: Column index where the category was found
            
        Returns:
            Tuple containing the two values found (year_x_val, year_y_val)
        """
        if field in self.config['boolean_fields']:
            return self._extract_boolean_value(row, category_col)
        
        year_x_val = None
        year_y_val = None
        # Fixed offsets from category column
        year_x_col = category_col + 2
        year_y_col = category_col + 4
        

        # Extract values from the specific columns
        if year_x_col < len(row):
            val = row[year_x_col]
            if pd.notna(val):
                if isinstance(val, str):
                    # Handle currency values
                    val = str(val).replace('€', '').replace('.', '').replace(',', '.').strip()
                try:
                    year_x_val = float(val)
                except (ValueError, TypeError):
                    pass
                    
        if year_y_col < len(row):
            val = row[year_y_col]
            if pd.notna(val):
                if isinstance(val, str):
                    # Handle currency values
                    val = str(val).replace('€', '').replace('.', '').replace(',', '.').strip()
                try:
                    year_y_val = float(val)
                except (ValueError, TypeError):
                    pass
        
        return year_x_val, year_y_val

    def _extract_boolean_value(self, row: pd.Series, category_col: int) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract boolean (Ja/Nein) values from fixed column offsets relative to category column.
        
        Args:
            row: DataFrame row to extract values from
            category_col: Column index where the category was found
            
        Returns:
            Tuple containing the two values found (year_x_val, year_y_val)
        """
        year_x_val = None
        year_y_val = None
        
        # Fixed offsets from category column
        year_x_col = category_col + 2
        year_y_col = category_col + 4
        
        if year_x_col < len(row):
            val = row[year_x_col]
            if pd.notna(val):
                val_str = str(val).strip().lower()
                if val_str in ['ja', 'nein']:
                    year_x_val = val_str.capitalize()
                    
        if year_y_col < len(row):
            val = row[year_y_col]
            if pd.notna(val):
                val_str = str(val).strip().lower()
                if val_str in ['ja', 'nein']:
                    year_y_val = val_str.capitalize()
        
        return year_x_val, year_y_val

    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract Verpflegung data from Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted and transformed data
        """
        try:
            self.logger.info(f"Starting data extraction from {file_path}")
            
            # Find the correct sheet
            xl = pd.ExcelFile(file_path)
            sheet_names = xl.sheet_names
            self.logger.info(f"Available sheets: {sheet_names}")
            
            sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
            self.logger.info(f"Found sheet: {sheet_name}")
            
            # Read the entire sheet
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            self.logger.info(f"Read sheet with shape: {df.shape}")
            
            # Save entire sheet for debugging
            df.to_csv('debug_verpflegung_full.csv')
            self.logger.info("Saved full sheet to debug_verpflegung_full.csv")
            
            # Find the starting row of Verpflegung section
            start_row = self._find_section_start(df, self.config['section_marker'])
            self.logger.info(f"Found section start at row: {start_row}")
            
            if start_row is None:
                raise ValueError(f"Could not find '{self.config['section_marker']}' section")
            
            # Get the years
            years = self._find_years(df, start_row)
            if years is None:
                # Create empty DataFrame with correct columns
                empty_df = pd.DataFrame(columns=self.config['output_columns'])
                self.logger.warning(f"No years found in {file_path}, skipping file")
                return empty_df
            
            year_x, year_y = years
            self.logger.info(f"Found years: {year_x}, {year_y}")
            
            # Initialize data storage with all required columns
            data = []
            
            # Process each row looking for the defined fields
            current_row = start_row
            while current_row < len(df):
                row = df.iloc[current_row]
                
                # Check each cell in the row for matching fields
                for col_idx, cell in enumerate(row):
                    if pd.notna(cell):
                        cell_str = str(cell).strip()
                        # Check if this cell contains any of our expected rows
                        for field in self.config['verpflegung_rows']:
                            if field.lower() in cell_str.lower():
                                year_x_val, year_y_val = self._extract_value(row, field, col_idx)
                                
                                # Create a row with all required columns
                                row_data = {
                                    'category': field,
                                    'source_file': Path(file_path).stem
                                }
                                # Add year columns with proper names
                                row_data[f'year_{year_x}'] = year_x_val
                                row_data[f'year_{year_y}'] = year_y_val
                                
                                data.append(row_data)
                                self.logger.debug(f"Found field: {field}")
                                self.logger.debug(f"Values: {year_x_val}, {year_y_val}")
                
                current_row += 1
            
            # Create DataFrame from the collected data
            result_df = pd.DataFrame(data)
            
            if len(result_df) == 0:
                raise ValueError("No Verpflegung data found in the file")
            
            # Ensure all required columns exist
            for col in self.config['output_columns']:
                if col not in result_df.columns:
                    result_df[col] = None
            
            # Ensure output columns are in the correct order
            result_df = result_df[self.config['output_columns']]
            
            self.logger.info(f"Extracted {len(result_df)} rows of data")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error extracting Verpflegung data: {str(e)}")
            raise 