"""
Concrete implementation of Excel extractor for kindergarten data.
"""

from pathlib import Path
import pandas as pd
from typing import List, Dict, Tuple, Optional

from .base_extractor import BaseExcelExtractor

class KindergartenExcelExtractor(BaseExcelExtractor):
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract kindergarten data from Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted and transformed data
            
        Raises:
            ValueError: If required sections are not found in config
        """
        self.logger.info(f"Starting data extraction from {file_path}")
        
        # Validate config structure
        required_sections = ['section_a_structure', 'section_b_structure']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required section '{section}' in config")
        
        # Extract sections
        sections_data = []
        for section_name in required_sections:
            section_df = self._extract_section(
                file_path=file_path,
                structure=self.config[section_name]
            )
            sections_data.append(section_df)
            self.logger.info(f"{section_name} extracted, got {len(section_df)} rows")
        
        # Combine results
        result = pd.concat(sections_data, ignore_index=True)
        self.logger.info(f"Combined data has {len(result)} rows")
        
        return result

    @staticmethod
    def _normalize_text(text: str | float | None) -> str:
        """
        Normalize text by removing extra whitespace and handling NaN values.
        
        Args:
            text: Text to normalize
            
        Returns:
            str: Normalized text
        """
        if pd.isna(text):
            return ''
        return ' '.join(str(text).split())

    def _find_category_position(
        self,
        df: pd.DataFrame,
        category: str,
        log_partial_matches: bool = True
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Find the position (row and column) of a category in the DataFrame.
        
        Args:
            df: DataFrame to search in
            category: Category to find
            log_partial_matches: Whether to log partial matches for debugging
            
        Returns:
            Tuple[Optional[int], Optional[int]]: Row and column indices, or (None, None) if not found
        """
        normalized_category = self._normalize_text(category)
        
        for col in df.columns:
            mask = df[col].apply(self._normalize_text) == normalized_category
            if mask.any():
                return mask.idxmax(), df.columns.get_loc(col)
        
        if log_partial_matches:
            self._log_partial_matches(df, category)
                
        return None, None

    def _log_partial_matches(self, df: pd.DataFrame, category: str) -> None:
        """
        Log partial matches for debugging purposes.
        
        Args:
            df: DataFrame to search in
            category: Category to find partial matches for
        """
        normalized_category = self._normalize_text(category)
        self.logger.info("No exact match found, looking for partial matches:")
        
        for idx, row in df.iterrows():
            for col in df.columns:
                val = row[col]
                normalized_val = self._normalize_text(val)
                if normalized_val and (normalized_category in normalized_val or normalized_val in normalized_category):
                    self.logger.info(f"Found partial match at row {idx}, col {col}: '{normalized_val}'")

    def _get_preview_data(
        self,
        file_path: Path | str,
        sheet_name: str,
        nrows: int = 50
    ) -> pd.DataFrame:
        """
        Read preview data from Excel file.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Name of sheet to read
            nrows: Number of rows to read
            
        Returns:
            pd.DataFrame: Preview data
        """
        return pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            nrows=nrows,
            header=None
        )

    def _transform_data(
        self,
        df: pd.DataFrame,
        structure: Dict,
        file_path: Path | str
    ) -> pd.DataFrame:
        """
        Transform the extracted data according to the structure.
        
        Args:
            df: DataFrame to transform
            structure: Structure definition from config
            file_path: Source file path for reference
            
        Returns:
            pd.DataFrame: Transformed data
        """
        transformed_rows = []
        
        for main_category, subcategories in structure.items():
            for subcategory in subcategories:
                found = False
                normalized_subcategory = self._normalize_text(subcategory)
                
                for col in df.columns:
                    mask = df[col].apply(self._normalize_text) == normalized_subcategory
                    if mask.any():
                        row = df[mask].iloc[0]
                        transformed_rows.append({
                            'category': f"{main_category} - {subcategory}",
                            'value_2022': row[df.columns[1]] if len(df.columns) > 1 else None,
                            'value_2023': row[df.columns[2]] if len(df.columns) > 2 else None,
                            'source_file': Path(file_path).name
                        })
                        found = True
                        break
                
                if not found:
                    self.logger.warning(f"Subcategory '{subcategory}' not found in data")
        
        return pd.DataFrame(transformed_rows)

    def _extract_section(self, file_path: str | Path, structure: dict) -> pd.DataFrame:
        """Extract a section from the Excel file."""
        self.logger.info(f"Extracting section from {file_path}")
        self.logger.debug(f"Parameters: structure={structure}")
        
        # Find the correct sheet
        xl = pd.ExcelFile(file_path)
        sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
        
        # Get preview data to find the starting position
        preview_df = self._get_preview_data(file_path, sheet_name)
        self.logger.info(f"Preview DataFrame shape: {preview_df.shape}")
        
        # Find the starting position using the first category
        first_category = next(iter(structure))
        start_row, category_column = self._find_category_position(preview_df, first_category)
        
        if start_row is None:
            self._log_partial_matches(preview_df, first_category)
            raise ValueError(f"Could not find starting category '{first_category}' in the file")
            
        self.logger.info(f"Found starting row at index {start_row}")
        
        # Determine columns to use
        columns_to_use = list(range(category_column, min(category_column + 4, len(preview_df.columns))))
        self.logger.info(f"Using columns: {columns_to_use}")
        
        # Read and transform the actual data
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            skiprows=start_row,
            usecols=columns_to_use,
            header=None
        )
        
        self.logger.debug(f"Raw data shape: {df.shape}")
        
        return self._transform_data(df, structure, file_path) 