"""
Base class for Excel data extraction.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import pandas as pd
import yaml
import logging
import fnmatch

class BaseExcelExtractor(ABC):
    def __init__(self, config_path: str | Path):
        """
        Initialize base extractor with configuration.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = logging.getLogger('deckblatt')

    @staticmethod
    def _load_config(config_path: str | Path) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _find_matching_sheet(self, xl: pd.ExcelFile, patterns: List[str]) -> str:
        """
        Find the first sheet name that matches any of the given patterns.
        
        Args:
            xl: Pandas ExcelFile object
            patterns: List of patterns to match against
            
        Returns:
            str: Name of the first matching sheet
            
        Raises:
            ValueError: If no matching sheet is found
        """
        self.logger.debug(f"Available sheets: {xl.sheet_names}")
        self.logger.debug(f"Looking for patterns: {patterns}")
        
        for sheet_name in xl.sheet_names:
            self.logger.debug(f"Checking sheet: {sheet_name}")
            for pattern in patterns:
                self.logger.debug(f"  Against pattern: {pattern}")
                if fnmatch.fnmatch(sheet_name.upper(), pattern.upper()):
                    self.logger.info(f"Found matching sheet: {sheet_name} (matched pattern: {pattern})")
                    return sheet_name
        
        raise ValueError(f"No sheet matching patterns {patterns} found. Available sheets: {xl.sheet_names}")

    def validate_excel_file(self, file_path: str | Path) -> bool:
        """
        Validate if Excel file meets required structure.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            bool: True if valid
            
        Raises:
            ValueError: If file structure is invalid
            FileNotFoundError: If file doesn't exist
        """
        try:
            self.logger.info(f"Validating file: {file_path}")
            xl = pd.ExcelFile(file_path)
            sheet_patterns = self.config.get('sheet_patterns', [])
            
            if not sheet_patterns:
                raise ValueError("No sheet patterns defined in configuration")
            
            # Try to find at least one matching sheet
            matching_sheet = self._find_matching_sheet(xl, sheet_patterns)
            self.logger.info(f"Validation successful, found matching sheet: {matching_sheet}")
            return True
            
        except Exception as e:
            self.logger.error(f"Validation failed for {file_path}: {str(e)}")
            raise

    def _find_row_index(self, df: pd.DataFrame, target_text: str) -> int:
        """
        Find the row index containing the target text.
        
        Args:
            df: DataFrame to search in
            target_text: Text to find
            
        Returns:
            int: Row index if found, -1 if not found
        """
        for idx, row in df.iterrows():
            for cell in row:
                if isinstance(cell, str) and cell.strip() == target_text.strip():
                    return idx
        return -1

    def _extract_row_values(self, df: pd.DataFrame, row_idx: int, start_col: int) -> Tuple[float, float, float]:
        """
        Extract values from a row starting at a specific column.
        
        Args:
            df: DataFrame to extract from
            row_idx: Row index to extract from
            start_col: Starting column index
            
        Returns:
            Tuple[float, float, float]: Values for 2022, 2023, and difference
        """
        value_2022 = df.iloc[row_idx, start_col + 1]
        value_2023 = df.iloc[row_idx, start_col + 2]
        abweichung = df.iloc[row_idx, start_col + 3]
        return value_2022, value_2023, abweichung

    def _find_start_column(self, df: pd.DataFrame, row_idx: int, target_text: str) -> Optional[int]:
        """
        Find the starting column for a given row and target text.
        
        Args:
            df: DataFrame to search in
            row_idx: Row index to search in
            target_text: Text to find
            
        Returns:
            Optional[int]: Column index if found, None if not found
        """
        row_data = df.iloc[row_idx]
        for col_idx, value in enumerate(row_data):
            if isinstance(value, str) and value.strip() == target_text.strip():
                return col_idx
        return None

    def _extract_section(
        self,
        file_path: str | Path,
        structure: Dict,
    ) -> pd.DataFrame:
        """
        Extract data from an Excel section based on structure.
        
        Args:
            file_path: Path to Excel file
            structure: Dictionary defining the data structure to look for
            
        Returns:
            pd.DataFrame: Extracted data with columns [level_1, level_2, value_2022, value_2023, abweichung]
            
        Raises:
            ValueError: If data extraction fails
        """
        try:
            self.logger.info(f"Extracting section from {file_path}")
            self.logger.debug(f"Processing structure configuration: {structure}")
            
            xl = pd.ExcelFile(file_path)
            sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            data = []
            
            for level_1, level_2_items in structure.items():
                self.logger.info(f"Processing level 1: {level_1}")
                level_1_row = self._find_row_index(df, level_1)
                
                if level_1_row != -1:
                    self.logger.info(f"Found level 1 '{level_1}' at row {level_1_row}")
                    
                    for level_2 in level_2_items:
                        level_2_row = self._find_row_index(df, level_2)
                        
                        if level_2_row != -1:
                            self.logger.info(f"Found level 2 '{level_2}' at row {level_2_row}")
                            start_col = self._find_start_column(df, level_2_row, level_2)
                            
                            if start_col is not None:
                                value_2022, value_2023, abweichung = self._extract_row_values(
                                    df, level_2_row, start_col
                                )
                                
                                data.append({
                                    'level_1': level_1,
                                    'level_2': level_2,
                                    'value_2022': value_2022,
                                    'value_2023': value_2023,
                                    'abweichung': abweichung,
                                    'source_file': Path(file_path).name
                                })
                                
                                self.logger.debug(
                                    f"Extracted values: 2022={value_2022}, "
                                    f"2023={value_2023}, abweichung={abweichung}"
                                )
            
            result_df = pd.DataFrame(data)
            self.logger.info(f"Extracted {len(result_df)} rows of data")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error extracting section from {file_path}: {str(e)}")
            self.logger.exception("Detailed error information:")
            raise

    def _handle_processing_error(self, file_path: Path, error: Exception) -> Dict:
        """
        Handle and format processing errors.
        
        Args:
            file_path: Path to file that caused the error
            error: Exception that occurred
            
        Returns:
            Dict: Formatted error information
        """
        return {
            'file_name': file_path.name,
            'error_type': type(error).__name__,
            'error_message': str(error)
        }

    @abstractmethod
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract data from Excel file. Must be implemented by subclasses.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted data
        """
        pass

    def process_files(
        self,
        directory_path: str | Path,
        file_pattern: str = "*.xlsx",
        checkpoint_file: Optional[str | Path] = None,
        debug_limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Process multiple Excel files in a directory.
        
        Args:
            directory_path: Path to directory containing Excel files
            file_pattern: Pattern to match Excel files
            checkpoint_file: Path to checkpoint file (optional)
            debug_limit: Limit number of files to process (optional)
            
        Returns:
            pd.DataFrame: Combined results from all processed files
            
        Raises:
            FileNotFoundError: If no files found
            ValueError: If no files processed successfully
        """
        directory_path = Path(directory_path)
        file_paths = list(directory_path.glob(file_pattern))
        
        if not file_paths:
            raise FileNotFoundError(f"No Excel files found in {directory_path}")
        
        if debug_limit:
            file_paths = file_paths[:debug_limit]
            self.logger.info(f"Debug mode: processing only {debug_limit} files")
        
        all_results = []
        errors = []
        
        for file_path in file_paths:
            try:
                self.logger.info(f"Processing file: {file_path.name}")
                if self.validate_excel_file(file_path):
                    df = self.extract_data(file_path)
                    if len(df) > 0:
                        all_results.append(df)
                        self.logger.info(f"Successfully extracted {len(df)} rows from {file_path.name}")
                    else:
                        self.logger.warning(f"No data extracted from {file_path.name}")
            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {str(e)}")
                errors.append(self._handle_processing_error(file_path, e))
        
        if errors:
            error_df = pd.DataFrame(errors)
            error_path = directory_path / f"errors_{self.__class__.__name__}.csv"
            error_df.to_csv(error_path, index=False)
            self.logger.warning(f"Errors logged to: {error_path}")
            self.logger.warning(f"Files with errors: {[e['file_name'] for e in errors]}")
        
        if not all_results:
            self.logger.error("No files were successfully processed")
            if errors:
                self.logger.error("All files failed due to errors. Check the error log for details.")
            raise ValueError("No files were successfully processed")
        
        final_df = pd.concat(all_results, ignore_index=True)
        self.logger.info(f"Final dataset has {len(final_df)} rows from {final_df['source_file'].nunique()} files")
        return final_df 