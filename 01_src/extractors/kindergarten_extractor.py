"""
Concrete implementation of Excel extractor for kindergarten data.
"""

from pathlib import Path
import pandas as pd

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
        self.logger.info(f"Starting data extraction from {file_path}")
        
        # Extract section A data
        section_a = self._extract_section(
            file_path=file_path,
            structure=self.config['section_a_structure']
        )
        self.logger.info(f"Section A extracted, got {len(section_a)} rows")
        
        # Extract section B data
        section_b = self._extract_section(
            file_path=file_path,
            structure=self.config['section_b_structure']
        )
        self.logger.info(f"Section B extracted, got {len(section_b)} rows")
        
        # Combine results
        result = pd.concat([section_a, section_b], ignore_index=True)
        self.logger.info(f"Combined data has {len(result)} rows")
        
        return result
    
    def _extract_section(self, file_path: str | Path, structure: dict) -> pd.DataFrame:
        """Extract a section from the Excel file."""
        self.logger.info(f"Extracting section from {file_path}")
        self.logger.debug(f"Parameters: structure={structure}")
        
        # Find the correct sheet
        xl = pd.ExcelFile(file_path)
        sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
        
        # First, read a portion of the file to find the starting row
        preview_df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            nrows=50,  # Read first 50 rows to find our section
            header=None  # Don't use first row as header
        )
        
        self.logger.info(f"Preview DataFrame shape: {preview_df.shape}")
        self.logger.info(f"Preview DataFrame columns: {preview_df.columns}")
        
        # Get the first main category from the structure
        first_category = next(iter(structure))
        self.logger.debug(f"Looking for first category: {first_category}")
        
        # Find the row where this category appears - with robust whitespace handling
        def normalize_text(text):
            if pd.isna(text):
                return ''
            return ' '.join(str(text).split())  # This normalizes all whitespace
            
        normalized_category = normalize_text(first_category)
        self.logger.info(f"Normalized category we're looking for: '{normalized_category}'")
        
        # Debug print all values in all columns
        self.logger.info("Values in all columns:")
        for idx, row in preview_df.iterrows():
            for col in preview_df.columns:
                val = row[col]
                normalized_val = normalize_text(val)
                if normalized_val:  # Only log non-empty values
                    self.logger.info(f"Row {idx}, Col {col}: Original='{val}', Normalized='{normalized_val}'")
            
        # Try to find the category in any column
        found = False
        start_row = None
        category_column = None
        for col in preview_df.columns:
            mask = preview_df[col].apply(normalize_text) == normalized_category
            if mask.any():
                start_row = mask.idxmax()
                category_column = col
                found = True
                self.logger.info(f"Found category in column {col} at row {start_row}")
                break
        
        if not found:
            # Try to find partial match for debugging
            self.logger.info("No exact match found, looking for partial matches:")
            for idx, row in preview_df.iterrows():
                for col in preview_df.columns:
                    val = row[col]
                    normalized_val = normalize_text(val)
                    if normalized_category in normalized_val:
                        self.logger.info(f"Found partial match at row {idx}, col {col}: '{normalized_val}'")
                    elif normalized_val and normalized_val in normalized_category:
                        self.logger.info(f"Found reverse partial match at row {idx}, col {col}: '{normalized_val}'")
            raise ValueError(f"Could not find starting category '{first_category}' in the file")
            
        self.logger.info(f"Found starting row at index {start_row}")
        
        # Determine the columns to use based on where we found the category
        columns_to_use = list(range(category_column, min(category_column + 4, len(preview_df.columns))))
        self.logger.info(f"Using columns: {columns_to_use}")
        
        # Now read the actual data starting from the identified row
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            skiprows=start_row,
            usecols=columns_to_use,
            header=None  # Don't use first row as header
        )
        
        self.logger.debug(f"Raw data shape: {df.shape}")
        self.logger.debug(f"Raw data columns: {df.columns}")
        self.logger.debug("First few rows:")
        self.logger.debug(f"{df.head()}")
        
        # Create a list to store transformed rows
        transformed_rows = []
        
        # Iterate through the structure to extract data - using same text normalization
        for main_category, subcategories in structure.items():
            for subcategory in subcategories:
                # Find the row containing this subcategory, handling NaN values
                found = False
                for col in df.columns:
                    normalized_subcategory = normalize_text(subcategory)
                    mask = df[col].apply(normalize_text) == normalized_subcategory
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
        
        # Convert to DataFrame
        result_df = pd.DataFrame(transformed_rows)
        
        return result_df 