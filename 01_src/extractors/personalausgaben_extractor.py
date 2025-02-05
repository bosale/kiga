from pathlib import Path
import pandas as pd
from .base_extractor import BaseExtractor
from utils import find_sheet_with_content, extract_section_data


class PersonalausgabenExtractor(BaseExtractor):
    """
    Extractor for personnel expenses data from kindergarten Excel files.
    Extracts data about staff costs, including salaries, social security contributions,
    and other personnel-related expenses.
    """
    
    def __init__(self, structure: dict):
        """
        Initialize the PersonalausgabenExtractor.
        
        Args:
            structure: Dictionary containing the structure definition for personnel expenses
        """
        super().__init__(structure)
        
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract personnel expenses data from a single Excel file.
        
        Args:
            file_path: Path to the Excel file to process
            
        Returns:
            pd.DataFrame: Extracted personnel expenses data with columns:
                - source_file: Name of processed file
                - category: Main category (I. PERSONALAUSGABEN)
                - subcategory: Section (e.g. BETREUUNGSPERSONAL)
                - subcategory_desc: Description of the subcategory
                - detail: Specific item details
                - value_2022: Value for year 2022
                - value_2023: Value for year 2023
                - comment: Additional comments if any
                
        Raises:
            ValueError: If required sections are not found in the file
        """
        self.logger.info(f"\nProcessing file: {file_path}")
        
        target_sheet = find_sheet_with_content(file_path, 'A. AUSGABEN')
        self.logger.info(f"Found sheet: {target_sheet}")
        
        if target_sheet is None:
            raise ValueError(f"No sheet containing 'A. AUSGABEN' found in {file_path}")
        
        df = pd.read_excel(file_path, sheet_name=target_sheet, header=None)
        self.logger.debug(f"DataFrame shape: {df.shape}")
        self.logger.debug("First few rows of DataFrame:")
        self.logger.debug(df.head())
        
        try:
            result = extract_section_data(df, 'I', self.structure, file_path, self.logger)
            self.logger.debug(f"Extracted {len(result)} rows")
            return result
        except Exception as e:
            self.logger.error(f"Error in extract_section_data: {str(e)}")
            self.logger.error(f"Structure used: {self.structure}")
            raise 