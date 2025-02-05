"""
Base class for Excel data extractors.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional, Tuple
import logging
import traceback

class BaseExcelExtractor:
    def __init__(self, config: Dict, logger: Optional[logging.Logger] = None):
        """
        Initialize the extractor.
        
        Args:
            config: Configuration dictionary
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    def _get_preview_data(self, file_path: str | Path, sheet_name: str, nrows: int = 100) -> pd.DataFrame:
        """Get preview data from Excel file."""
        try:
            return pd.read_excel(str(file_path), sheet_name=sheet_name, nrows=nrows)
        except Exception as e:
            self.logger.error(f"Error reading preview data: {str(e)}")
            raise

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
        try:
            directory_path = Path(directory_path)
            self.logger.debug(f"Looking for Excel files in {directory_path} with pattern {file_pattern}")
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
                    df = self.extract_data(file_path)
                    if len(df) > 0:
                        all_results.append(df)
                        self.logger.info(f"Successfully extracted {len(df)} rows from {file_path.name}")
                    else:
                        self.logger.warning(f"No data extracted from {file_path.name}")
                except Exception as e:
                    self.logger.error(f"Error processing {file_path.name}: {str(e)}")
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
                    errors.append(self._handle_processing_error(file_path, e))
            
            if errors:
                error_df = pd.DataFrame(errors)
                error_path = directory_path / f"errors_{self.__class__.__name__}.csv"
                error_df.to_csv(str(error_path), index=False)
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
            
        except Exception as e:
            self.logger.error(f"Error in process_files: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _handle_processing_error(self, file_path: Path | str, error: Exception) -> Dict:
        """
        Handle and format processing errors.
        
        Args:
            file_path: Path to file that caused the error
            error: Exception that occurred
            
        Returns:
            Dict: Formatted error information
        """
        file_path = Path(file_path)
        return {
            'file_name': file_path.name,
            'error_type': type(error).__name__,
            'error_message': str(error)
        }

    def _find_matching_sheet(self, xl: pd.ExcelFile, patterns: List[str]) -> str:
        """Find first sheet name matching any of the patterns."""
        try:
            for sheet in xl.sheet_names:
                for pattern in patterns:
                    if str(pattern).upper() in str(sheet).upper():
                        return sheet
            raise ValueError(f"No sheet matching patterns {patterns}")
        except Exception as e:
            self.logger.error(f"Error finding matching sheet: {str(e)}")
            raise

    @staticmethod
    def _normalize_text(text: str | float | None) -> str:
        """Normalize text by removing extra whitespace and handling NaN values."""
        if pd.isna(text):
            return ''
        return ' '.join(str(text).split())

    def _find_section_start(self, df: pd.DataFrame, marker: str) -> Optional[int]:
        """Find the row index where a section starts."""
        try:
            for idx, row in df.iterrows():
                if any(isinstance(val, str) and str(marker).upper() in str(val).upper() 
                       for val in row.values if pd.notna(val)):
                    return idx
            return None
        except Exception as e:
            self.logger.error(f"Error finding section start: {str(e)}")
            return None

    def _find_category_position(
        self,
        df: pd.DataFrame,
        category: str,
        log_partial_matches: bool = True
    ) -> Tuple[Optional[int], Optional[int]]:
        """Find the position (row and column) of a category in the DataFrame."""
        normalized_category = self._normalize_text(category)
        
        for col in df.columns:
            mask = df[col].apply(self._normalize_text) == normalized_category
            if mask.any():
                return mask.idxmax(), df.columns.get_loc(col)
        
        if log_partial_matches:
            self._log_partial_matches(df, category)
                
        return None, None

    def _log_partial_matches(self, df: pd.DataFrame, category: str) -> None:
        """Log partial matches for debugging purposes."""
        normalized_category = self._normalize_text(category)
        self.logger.info("No exact match found, looking for partial matches:")
        
        for idx, row in df.iterrows():
            for col in df.columns:
                val = row[col]
                normalized_val = self._normalize_text(val)
                if normalized_val and (normalized_category in normalized_val or normalized_val in normalized_category):
                    self.logger.info(f"Found partial match at row {idx}, col {col}: '{normalized_val}'")

    def validate_config_sections(self, required_sections: List[str]) -> None:
        """Validate that required sections exist in config."""
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required section '{section}' in config")

    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract data from Excel file. To be implemented by child classes.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted and transformed data
        """
        raise NotImplementedError("Child classes must implement extract_data method")