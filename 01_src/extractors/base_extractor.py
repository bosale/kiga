"""
Base class for Excel data extractors.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional, Tuple
import logging
import traceback
from datetime import datetime
from utils.checkpoint_utils import handle_problematic_files

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
        self.issues = []  # List to track all issues (errors and warnings)

    def _get_preview_data(self, file_path: str | Path, sheet_name: str, nrows: int = 100) -> pd.DataFrame:
        """Get preview data from Excel file."""
        try:
            return pd.read_excel(str(file_path), sheet_name=sheet_name, nrows=nrows)
        except Exception as e:
            self.logger.error(f"Error reading preview data: {str(e)}")
            raise

    def _log_issue(self, file_path: Path | str, issue_type: str, message: str, details: Optional[Dict] = None) -> None:
        """
        Log an issue with a file.
        
        Args:
            file_path: Path to the problematic file
            issue_type: Type of issue (e.g., 'ERROR', 'WARNING', 'NO_DATA')
            message: Description of the issue
            details: Additional details about the issue (optional)
        """
        file_path = Path(file_path)
        issue = {
            'timestamp': datetime.now().isoformat(),
            'file_name': file_path.name,
            'issue_type': issue_type,
            'message': message,
            'extractor_type': self.__class__.__name__,
            'file_path': str(file_path)
        }
        if details:
            issue.update(details)
        self.issues.append(issue)
        
        # Log the issue
        log_method = self.logger.error if issue_type == 'ERROR' else self.logger.warning
        log_method(f"{issue_type} in {file_path.name}: {message}")

    def _handle_processing_error(self, file_path: Path | str, error: Exception) -> Dict:
        """
        Handle and format processing errors.
        
        Args:
            file_path: Path to file that caused the error
            error: Exception that occurred
            
        Returns:
            Dict: Formatted error information
        """
        error_info = {
            'file_name': Path(file_path).name,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc()
        }
        self._log_issue(file_path, 'ERROR', str(error), error_info)
        return error_info

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
            
            total_files = len(file_paths)
            self.logger.info(f"Found {total_files} files to process")
            
            all_results = []
            
            for idx, file_path in enumerate(file_paths, 1):
                try:
                    self.logger.info(f"Processing file [{idx}/{total_files}]: {file_path.name}")
                    df = self.extract_data(file_path)
                    if len(df) > 0:
                        all_results.append(df)
                        self.logger.info(f"Successfully extracted {len(df)} rows from {file_path.name}")
                    else:
                        self._log_issue(file_path, 'NO_DATA', 'No data was extracted from this file')
                except Exception as e:
                    self._handle_processing_error(file_path, e)
            
            # Handle problematic files using the existing utility
            if self.issues:
                # Convert issues to the format expected by handle_problematic_files
                problematic_files = [{
                    'file_name': issue['file_name'],
                    'file_path': issue['file_path'],
                    'issue_type': issue['issue_type'],
                    'message': issue['message'],
                    'timestamp': issue['timestamp'],
                    'extractor': issue['extractor_type']
                } for issue in self.issues]
                
                handle_problematic_files(problematic_files, directory_path, self.__class__.__name__)
                
                # Log summary of issues
                issue_summary = pd.DataFrame(self.issues)['issue_type'].value_counts()
                self.logger.warning("\nIssue Summary:")
                for issue_type, count in issue_summary.items():
                    self.logger.warning(f"{issue_type}: {count} files")
            
            if not all_results:
                self.logger.error("No files were successfully processed")
                if self.issues:
                    self.logger.error("All files failed. Check the problematic files log for details.")
                raise ValueError("No files were successfully processed")
            
            final_df = pd.concat(all_results, ignore_index=True)
            self.logger.info(f"\nProcessing complete!")
            self.logger.info(f"Successfully processed: {final_df['source_file'].nunique()}/{total_files} files")
            self.logger.info(f"Total records extracted: {len(final_df)}")
            return final_df
            
        except Exception as e:
            self.logger.error(f"Error in process_files: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _find_matching_sheet(self, xl: pd.ExcelFile, patterns: List[str]) -> List[str]:
        """
        Find sheet names matching the patterns, using exact match first, then fuzzy match.
        Returns a list of matching sheets.
        """
        try:
            matching_sheets = []
            
            # Step 1: Try exact matching first
            for sheet in xl.sheet_names:
                for pattern in patterns:
                    if str(sheet).upper() == str(pattern).upper():
                        self.logger.info(f"Found exact match for sheet: {sheet}")
                        return [sheet]  # Return single exact match immediately
            
            # Step 2: If no exact match, try fuzzy matching with "Standortinformation"
            if "Standortinformation" in patterns:
                fuzzy_matches = []
                for sheet in xl.sheet_names:
                    if "Standortinformation".upper() in str(sheet).upper():
                        fuzzy_matches.append(sheet)
                
                if fuzzy_matches:
                    self.logger.info(f"Found fuzzy match(es) for Standortinformation: {fuzzy_matches}")
                    return fuzzy_matches
            
            # Step 3: If still no match, try the original pattern matching
            for sheet in xl.sheet_names:
                for pattern in patterns:
                    if str(pattern).upper() in str(sheet).upper():
                        matching_sheets.append(sheet)
            
            if matching_sheets:
                self.logger.info(f"Found pattern match(es) for sheets: {matching_sheets}")
                return matching_sheets
            
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

    def process_sheet(self, file_path: Path, sheet_name: str) -> pd.DataFrame:
        """
        Process a single sheet from an Excel file.
        To be implemented by child classes.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet to process
            
        Returns:
            pd.DataFrame: Extracted data from the sheet
        """
        raise NotImplementedError("Child classes must implement process_sheet method")

    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract data from Excel file.
        This implementation handles multiple sheets and combines their results.
        Child classes should implement process_sheet instead.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted and transformed data
        """
        try:
            file_path = Path(file_path)
            xl = pd.ExcelFile(file_path)
            
            # Find matching sheets
            matching_sheets = self._find_matching_sheet(xl, self.config['sheet_patterns'])
            
            all_results = []
            for sheet_name in matching_sheets:
                try:
                    df = self.process_sheet(file_path, sheet_name)
                    if len(df) > 0:
                        # Add sheet name to source information
                        df['source_sheet'] = sheet_name
                        all_results.append(df)
                        self.logger.info(f"Successfully extracted {len(df)} rows from sheet '{sheet_name}'")
                    else:
                        self._log_issue(file_path, 'NO_DATA', f'No data was extracted from sheet {sheet_name}')
                except Exception as e:
                    self._log_issue(file_path, 'ERROR', f'Error processing sheet {sheet_name}: {str(e)}')
                    continue
            
            if not all_results:
                return pd.DataFrame()
                
            return pd.concat(all_results, ignore_index=True)
            
        except Exception as e:
            self._handle_processing_error(file_path, e)
            return pd.DataFrame()