"""
Concrete implementation of Excel extractor for kindergarten data.
"""

from pathlib import Path
import pandas as pd
from typing import List, Dict, Optional, Tuple
import traceback

from .base_extractor import BaseExcelExtractor

class KindergartenExcelExtractor(BaseExcelExtractor):
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract kindergarten data from Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted and transformed data
        """
        try:
            file_path = Path(file_path)  # Ensure file_path is a Path object
            self.logger.info(f"Starting data extraction from {file_path}")
            
            # Validate config structure
            self.validate_config_sections(['section_a_structure', 'section_b_structure'])
            
            # Find the correct sheet
            self.logger.debug(f"Opening Excel file: {str(file_path)}")
            xl = pd.ExcelFile(str(file_path))
            matching_sheets = self._find_matching_sheet(xl, self.config['sheet_patterns'])
            
            if not matching_sheets:
                raise ValueError(f"No matching sheets found in {file_path}")
            
            # Use the first matching sheet
            sheet_name = matching_sheets[0]
            self.logger.debug(f"Using sheet: {sheet_name}")
            
            # Read the entire sheet
            self.logger.debug(f"Reading sheet {sheet_name} from {str(file_path)}")
            df = pd.read_excel(str(file_path), sheet_name=sheet_name)
            self.logger.debug(f"DataFrame shape: {df.shape}")
            
            # Extract sections
            sections_data = []
            for section_name in ['section_a_structure', 'section_b_structure']:
                self.logger.debug(f"Processing section: {section_name}")
                structure = self.config[section_name]
                self.logger.debug(f"Structure: {structure}")
                section_data = self._extract_section(df.copy(), structure, file_path)
                sections_data.append(section_data)
                self.logger.info(f"{section_name} extracted, got {len(section_data)} rows")
            
            # Combine results
            if not sections_data:
                self.logger.warning("No sections data collected")
                return pd.DataFrame()
                
            result = pd.concat(sections_data, ignore_index=True)
            self.logger.info(f"Combined data has {len(result)} rows")
            
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
            
            for main_category, subcategories in structure.items():
                self.logger.debug(f"Processing main category: {main_category}")
                
                if not isinstance(subcategories, (list, tuple)):
                    self.logger.error(f"Invalid subcategories format for {main_category}: {type(subcategories)}")
                    continue
                    
                for subcategory in subcategories:
                    self.logger.debug(f"Processing subcategory: {subcategory}")
                    
                    # Find the row containing this subcategory
                    for col_idx, col in enumerate(df.columns):
                        try:
                            # Convert column to string series and clean it
                            col_series = df[col].fillna('').astype(str).str.strip()
                            subcategory_str = str(subcategory).strip()
                            
                            # Find matches
                            mask = col_series == subcategory_str
                            if mask.any():
                                row = df[mask].iloc[0]
                                
                                # Try to get values from the next columns
                                try:
                                    value_2022 = row.iloc[col_idx + 1]
                                    value_2023 = row.iloc[col_idx + 2]
                                    abweichung = row.iloc[col_idx + 3]
                                    
                                    entry = {
                                        'category': str(main_category),
                                        'subcategory': str(subcategory),
                                        'value_2022': value_2022,
                                        'value_2023': value_2023,
                                        'abweichung': abweichung,
                                        'source_file': file_path.name
                                    }
                                    data.append(entry)
                                    
                                    self.logger.debug(f"Found values for {subcategory}: {entry}")
                                    break
                                except IndexError:
                                    self.logger.warning(
                                        f"Found subcategory {subcategory} but couldn't extract all values"
                                    )
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