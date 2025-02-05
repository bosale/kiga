"""
Concrete implementation of Excel extractor for Schliesszeiten (closing times) data.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional, Tuple
import logging

from .base_extractor import BaseExcelExtractor

class SchliesszeitenExtractor(BaseExcelExtractor):
    """Extractor for Schliesszeiten (closing times) data from Excel files."""
    
    REQUIRED_COLUMNS = ["Kindergartenjahr", "Monat", "Schliesstage"]
    SKIP_EMPTY_ROWS = True
    
    def _find_year_row(self, df: pd.DataFrame, start_row: int) -> Tuple[Optional[int], List[int]]:
        """Find the row containing kindergarten years and their column positions."""
        year_cols = []
        year_row = None
        
        # Look for the first month to determine where data starts
        month_row = None
        for idx in range(start_row, min(start_row + 15, len(df))):
            row = df.iloc[idx]
            if any('SEPTEMBER' in str(val).upper().strip() for val in row.values if pd.notna(val)):
                month_row = idx
                break
                
        if month_row is None:
            return None, []
            
        # Look for kindergarten years in rows before the month row
        for row_idx in range(max(0, month_row - 3), month_row):
            row = df.iloc[row_idx]
            for col_idx in range(len(row)):
                val = str(row[col_idx]).strip() if pd.notna(row[col_idx]) else ''
                # Look for patterns like "2022/2023" or "2022/23" or "22/23"
                if '/' in val and any(char.isdigit() for char in val):
                    year_cols.append(col_idx)
                    year_row = row_idx
                    
        return year_row, year_cols
    
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract Schliesszeiten data from Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted and transformed data
        """
        try:
            self.logger.info(f"Starting data extraction from {file_path}")
            
            # Find the correct sheet
            xl = pd.ExcelFile(file_path)
            sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
            self.logger.info(f"Found sheet: {sheet_name}")
            
            # Read the entire sheet
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            
            # Find the starting row of Schliesszeiten section
            start_row = self._find_section_start(df, "C. SCHLIESSZEITEN")
            if start_row is None:
                raise ValueError("Could not find 'SCHLIESSZEITEN' section")
                
            # Find year row and columns
            year_row, year_cols = self._find_year_row(df, start_row)
            if not year_cols:
                raise ValueError("Could not find kindergarten years")
                
            # Initialize lists to store the data
            data = []
            
            # Find the row containing "September" to start processing months
            september_row = None
            for idx in range(start_row, min(start_row + 15, len(df))):
                row = df.iloc[idx]
                if any('SEPTEMBER' in str(val).upper().strip() for val in row.values if pd.notna(val)):
                    september_row = idx
                    break
                    
            if september_row is None:
                raise ValueError("Could not find row containing 'September'")
                
            # Process each kindergarten year
            for year_col in year_cols:
                kg_year = str(df.iloc[year_row, year_col]).strip()
                
                # Process each month
                for month_idx, month in enumerate(self.config['months']):
                    try:
                        # Read closing days from the year column
                        closing_days = df.iloc[september_row + month_idx, year_col + 1]
                        
                        # Only add entries where we have actual closing days
                        if pd.notna(closing_days) and str(closing_days).strip() != '':
                            try:
                                closing_days = int(float(str(closing_days).strip()))
                                data.append({
                                    'Kindergartenjahr': kg_year,
                                    'Monat': month,
                                    'Schliesstage': closing_days,
                                    'source_file': Path(file_path).stem
                                })
                            except ValueError:
                                self.logger.warning(
                                    f"Could not convert '{closing_days}' to integer "
                                    f"for {month} in {kg_year}"
                                )
                    except Exception as e:
                        self.logger.warning(
                            f"Error processing {month} for {kg_year}: {str(e)}"
                        )
            
            # Create DataFrame from the collected data
            result_df = pd.DataFrame(data)
            
            if len(result_df) == 0:
                raise ValueError("No Schliesszeiten data found in the file")
                
            # Ensure output columns are in the correct order
            result_df = result_df[self.config['output_columns']]
            
            self.logger.info(f"Extracted {len(result_df)} rows of data")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error extracting Schliesszeiten data: {str(e)}")
            raise 