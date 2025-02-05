"""
Concrete implementation of Excel extractor for Personalausgaben (personnel expenses) data.
"""

from pathlib import Path
import pandas as pd
from .base_extractor import BaseExcelExtractor
from utils import find_sheet_with_content, extract_section_data


class PersonalausgabenExtractor(BaseExcelExtractor):
    """
    Extractor for personnel expenses data from kindergarten Excel files.
    Extracts data about staff costs, including salaries, social security contributions,
    and other personnel-related expenses.
    """
    
    def __init__(self, config: dict):
        """
        Initialize the PersonalausgabenExtractor.
        
        Args:
            config: Dictionary containing the configuration for personnel expenses
        """
        super().__init__(config)
        
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
        
        # Find the correct sheet
        xl = pd.ExcelFile(file_path)
        self.logger.debug(f"Available sheets: {xl.sheet_names}")
        sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
        self.logger.info(f"Found sheet: {sheet_name}")
        
        if sheet_name is None:
            raise ValueError(f"No sheet matching patterns found in {file_path}")
        
        # Read the sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        self.logger.debug(f"DataFrame shape: {df.shape}")
        self.logger.debug("First few rows of data:")
        self.logger.debug(df.head(10).to_string())
        
        try:
            result = extract_section_data(
                df=df,
                section_identifier='I',
                structure=self.config,
                file_path=file_path,
                logger=self.logger,
                year_2022_col=3,  # Fixed column indices based on the actual data
                year_2023_col=4,
                comment_col=6
            )
            
            # Ensure output columns are in the correct order
            result = result[self.config['output_columns']]
            
            self.logger.debug(f"Extracted {len(result)} rows")
            self.logger.debug("Extracted data:")
            self.logger.debug(result.head().to_string())
            return result
        except Exception as e:
            self.logger.error(f"Error in extract_section_data: {str(e)}")
            self.logger.error(f"Configuration used: {self.config}")
            raise 