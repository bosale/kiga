"""
Concrete implementation of Excel extractor for Zusatzangaben (additional information) data.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional
import re

from .base_extractor import BaseExcelExtractor

class ZusatzangabenExtractor(BaseExcelExtractor):
    # Hardcoded validation rules
    REQUIRED_COLUMNS = ["Name_Eintrag", "Eintrag"]
    SKIP_EMPTY_ROWS = True
    TRIM_WHITESPACE = True
    
    def _normalize_question(self, question: str) -> str:
        """Normalize a question by removing whitespace, newlines, and special characters."""
        if not question or not isinstance(question, str):
            return ""
        return self._normalize_text(question) if self.TRIM_WHITESPACE else question
    
    def _generate_normalized_key(self, question: str) -> str:
        """Generate a normalized key from a question text."""
        # Remove special characters and convert to lowercase
        text = re.sub(r'[^\w\s]', '', question.lower())
        # Replace umlauts
        text = text.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss')
        # Split into words and take first few significant words
        words = [w for w in text.split() if w not in ['ist', 'das', 'die', 'der', 'und', 'oder', 'im', 'in', 'bei', 'zu', 'zur', 'zum']]
        key_words = words[:3] if len(words) > 3 else words
        # Join with underscores
        return '_'.join(key_words)
        
    def _find_matching_question(self, input_question: str) -> Optional[Dict]:
        """Find the matching predefined question for a given input question."""
        normalized_input = self._normalize_question(input_question)
        if not normalized_input:
            return None
            
        for question in self.config['zusatzangaben']:
            if self._normalize_question(question) == normalized_input:
                return {
                    'question': question,
                    'normalized': self._generate_normalized_key(question)
                }
        return None

    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract Zusatzangaben data from Excel file.
        
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
            
            # Initialize lists to store the data
            data = []
            unmatched_questions = []
            
            # Track which questions we've found
            found_questions = set()
            
            # Process all rows
            for idx, row in df.iterrows():
                # Get values from configured columns
                name_eintrag = str(row.iloc[self.config['columns']['name_eintrag']]).strip() if pd.notna(row.iloc[self.config['columns']['name_eintrag']]) else None
                eintrag = str(row.iloc[self.config['columns']['eintrag']]).strip() if pd.notna(row.iloc[self.config['columns']['eintrag']]) else None
                erlaeuterung = str(row.iloc[self.config['columns']['erlaeuterung']]).strip() if pd.notna(row.iloc[self.config['columns']['erlaeuterung']]) else None
                
                # Apply validation rules
                if self.SKIP_EMPTY_ROWS and not name_eintrag:
                    continue
                
                # Only process if we have a question
                if name_eintrag and name_eintrag != 'nan' and name_eintrag != '-':
                    # Find matching predefined question
                    matching_question = self._find_matching_question(name_eintrag)
                    
                    if matching_question:
                        found_questions.add(matching_question['question'])
                        row_data = {
                            'Name_Eintrag': name_eintrag,
                            'Eintrag': eintrag,
                            'Erlaeuterung': erlaeuterung if erlaeuterung and erlaeuterung != 'nan' else None,
                            'source_file': Path(file_path).stem,
                            'normalized_key': matching_question['normalized']
                        }
                        
                        # Validate required columns
                        if all(row_data.get(col) for col in self.REQUIRED_COLUMNS):
                            data.append(row_data)
                        else:
                            self.logger.warning(f"Row missing required columns: {row_data}")
                    else:
                        unmatched_questions.append(name_eintrag)
            
            # Check for missing questions
            all_questions = set(self.config['zusatzangaben'])
            missing_questions = all_questions - found_questions
            if missing_questions:
                self.logger.warning(f"Missing questions: {missing_questions}")
            
            # Log validation issues
            if unmatched_questions:
                self.logger.warning(f"Unmatched questions found: {unmatched_questions}")
            
            # Create DataFrame from the collected data
            result_df = pd.DataFrame(data)
            
            if len(result_df) == 0:
                raise ValueError("No Zusatzangaben data found in the file")
                
            # Ensure output columns are in the correct order
            result_df = result_df[self.config['output_columns']]
                
            self.logger.info(f"Extracted {len(result_df)} rows of data")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error extracting Zusatzangaben data: {str(e)}")
            raise 