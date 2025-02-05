"""
Concrete implementation of Excel extractor for Verbindlichkeiten data.
"""

from pathlib import Path
import pandas as pd
from typing import List, Dict, Optional, Tuple
import traceback

from .base_extractor import BaseExcelExtractor

class VerbindlichkeitenExtractor(BaseExcelExtractor):
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract Verbindlichkeiten data from Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted and transformed data
        """
        try:
            file_path = Path(file_path)
            self.logger.info(f"Starting data extraction from {file_path}")
            
            # Validate config structure
            self.validate_config_sections(['section_a_structure'])
            
            # Find the correct sheet
            self.logger.debug(f"Opening Excel file: {str(file_path)}")
            xl = pd.ExcelFile(str(file_path))
            sheet_name = self._find_matching_sheet(xl, self.config.get('sheet_patterns', ["NB_Vermögensübersicht"]))
            self.logger.debug(f"Found sheet: {sheet_name}")
            
            # Read the entire sheet
            self.logger.debug(f"Reading sheet {sheet_name} from {str(file_path)}")
            df = pd.read_excel(str(file_path), sheet_name=sheet_name, header=None)
            self.logger.debug(f"DataFrame shape: {df.shape}")
            
            # Extract section
            structure = self.config['section_a_structure']
            self.logger.debug(f"Structure: {structure}")
            result = self._extract_section(df.copy(), structure, file_path)
            self.logger.info(f"Extracted {len(result)} rows")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in extract_data: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _extract_section(
        self,
        df: pd.DataFrame,
        structure: Dict,
        file_path: Path,
    ) -> pd.DataFrame:
        """
        Extract data from a section based on structure.
        
        Args:
            df: DataFrame containing the data
            structure: Dictionary defining the data structure to look for
            file_path: Source file path for reference
            
        Returns:
            pd.DataFrame: Extracted data
        """
        try:
            data = []
            
            # Find the date row to get column indices for values
            date_row_mask = df.apply(lambda x: x.astype(str).str.contains('2023-01-01', na=False)).any(axis=1)
            if not date_row_mask.any():
                self.logger.warning("Could not find date row with '2023-01-01'")
                return pd.DataFrame()
                
            date_row_idx = date_row_mask.idxmax()
            date_row = df.iloc[date_row_idx]
            
            # Find column indices for start, end, and change values
            start_col_idx = None
            end_col_idx = None
            change_col_idx = None
            
            for idx, val in enumerate(date_row):
                if pd.notna(val):
                    if '2023-01-01' in str(val):
                        start_col_idx = idx
                    elif '2023-12-31' in str(val):
                        end_col_idx = idx
                    elif 'Veränderung' in str(val):
                        change_col_idx = idx
            
            if None in (start_col_idx, end_col_idx, change_col_idx):
                self.logger.warning("Could not find all required value columns")
                return pd.DataFrame()
            
            for main_category, items in structure.items():
                self.logger.debug(f"Processing main category: {main_category}")
                
                if not isinstance(items, (list, tuple)):
                    self.logger.error(f"Invalid items format for {main_category}: {type(items)}")
                    continue
                    
                for item in items:
                    self.logger.debug(f"Processing item: {item}")
                    
                    # Find the row containing this item
                    for col_idx, col in enumerate(df.columns):
                        try:
                            # Convert column to string series and clean it
                            col_series = df[col].fillna('').astype(str).str.strip()
                            item_str = str(item).strip()
                            
                            # Find matches
                            mask = col_series == item_str
                            if mask.any():
                                row = df[mask].iloc[0]
                                
                                # Get values using the correct column indices
                                entry = {
                                    'category': str(main_category),
                                    'item': str(item),
                                    'value_2023_start': row.iloc[start_col_idx],
                                    'value_2023_end': row.iloc[end_col_idx],
                                    'change': row.iloc[change_col_idx],
                                    'source_file': file_path.name
                                }
                                data.append(entry)
                                self.logger.debug(f"Found values for {item}: {entry}")
                                break
                                
                        except Exception as e:
                            self.logger.warning(f"Error processing column {col}: {str(e)}")
                            continue
            
            result_df = pd.DataFrame(data)
            if len(result_df) == 0:
                self.logger.warning(f"No data found for structure: {structure}")
            else:
                self.logger.debug(f"Extracted {len(result_df)} rows of data")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error in _extract_section: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise 