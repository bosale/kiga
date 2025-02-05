"""
Concrete implementation of Excel extractor for Öffnungszeiten (opening times) data.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional, Tuple

from .base_extractor import BaseExcelExtractor

class OeffnungszeitenExtractor(BaseExcelExtractor):
    """Extractor for Öffnungszeiten (opening times) data from Excel files."""
    
    def _find_table_structure(self, df: pd.DataFrame, start_row: int) -> Tuple[Optional[int], Dict[str, int]]:
        """
        Find the table structure including header row and column positions.
        
        Args:
            df: DataFrame containing the data
            start_row: Row where the section starts
            
        Returns:
            Tuple containing header row index and dictionary of column positions
        """
        header_row = None
        columns = {
            'group_col': None,
            'hours_col': None,
            'days_col': None,
            'hours_per_day_col': None,
            'time_range_col': None
        }
        
        # Look for header row by searching for specific column headers
        for idx in range(start_row, min(start_row + 15, len(df))):
            row = df.iloc[idx]
            
            for col, val in enumerate(row):
                if pd.isna(val):
                    continue
                val_str = str(val).upper().strip()
                
                # Look for key column identifiers
                if 'WOCHENTAG' in val_str:
                    columns['days_col'] = col
                    header_row = idx
                elif 'STUNDEN' in val_str and not any(x in val_str for x in ['Ø', 'DURCHSCHNITT']):
                    columns['hours_per_day_col'] = col
                    header_row = idx
                elif 'UHRZEIT' in val_str or ('VON' in val_str and 'BIS' in val_str):
                    columns['time_range_col'] = col
                    header_row = idx
                elif 'Ø STUNDEN' in val_str or 'DURCHSCHNITT' in val_str:
                    columns['hours_col'] = col
        
        # After finding header row, look for group column
        if header_row is not None:
            for idx in range(header_row + 1, min(header_row + 5, len(df))):
                row = df.iloc[idx]
                for col, val in enumerate(row):
                    if pd.notna(val) and str(val) in self.config['target_groups']:
                        columns['group_col'] = col
                        break
                if columns['group_col'] is not None:
                    break
        
        return header_row, columns
    
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract Öffnungszeiten data from Excel file.
        
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
            
            # Find the starting row of Öffnungszeiten section
            start_row = self._find_section_start(df, "D. ÖFFNUNGSZEITEN")
            if start_row is None:
                raise ValueError("Could not find 'ÖFFNUNGSZEITEN' section")
            
            # Find table structure
            header_row, columns = self._find_table_structure(df, start_row)
            
            if header_row is None or columns['group_col'] is None:
                self.logger.error("Header structure:")
                for idx in range(start_row, min(start_row + 15, len(df))):
                    self.logger.error(f"Row {idx}: {df.iloc[idx].tolist()}")
                raise ValueError("Could not identify table structure")
            
            # Initialize list to store the data
            data = []
            
            # Process each row after the header
            for idx in range(header_row + 1, len(df)):
                if pd.isna(df.iloc[idx, columns['group_col']]):
                    continue
                    
                group_name = str(df.iloc[idx, columns['group_col']])
                
                if group_name in self.config['target_groups']:
                    row_data = {
                        'Gruppe': group_name,
                        'Stunden_pro_Woche': df.iloc[idx, columns['hours_col']] if columns['hours_col'] is not None and pd.notna(df.iloc[idx, columns['hours_col']]) else None,
                        'Wochentage': df.iloc[idx, columns['days_col']] if columns['days_col'] is not None and pd.notna(df.iloc[idx, columns['days_col']]) else None,
                        'Stunden_pro_Tag': df.iloc[idx, columns['hours_per_day_col']] if columns['hours_per_day_col'] is not None and pd.notna(df.iloc[idx, columns['hours_per_day_col']]) else None,
                        'Oeffnungszeiten': df.iloc[idx, columns['time_range_col']] if columns['time_range_col'] is not None and pd.notna(df.iloc[idx, columns['time_range_col']]) else None,
                        'source_file': Path(file_path).stem
                    }
                    self.logger.debug(f"Found group: {group_name}")
                    self.logger.debug(f"Row data: {row_data}")
                    data.append(row_data)
            
            # Create DataFrame from the collected data
            result_df = pd.DataFrame(data)
            
            if len(result_df) == 0:
                raise ValueError("No Öffnungszeiten data found in the file")
            
            # Ensure output columns are in the correct order
            result_df = result_df[self.config['output_columns']]
            
            self.logger.info(f"Extracted {len(result_df)} rows of data")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error extracting Öffnungszeiten data: {str(e)}")
            raise 