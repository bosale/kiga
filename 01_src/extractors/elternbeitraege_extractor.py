"""
Concrete implementation of Excel extractor for Elternbeiträge (parent contributions) data.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional
import numpy as np
import logging

from .base_extractor import BaseExcelExtractor

class ElternbeitraegeExtractor(BaseExcelExtractor):
    def __init__(self, config: Dict, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        # Validate config structure at initialization
        self.validate_config_sections(['verpflegung_structure', 'zusatzleistungen_structure', 'section_markers'])

    def process_sheet(self, file_path: Path, sheet_name: str) -> pd.DataFrame:
        """
        Process a single sheet from an Excel file.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet to process
            
        Returns:
            pd.DataFrame: Extracted data from the sheet
        """
        self.logger.info(f"Processing sheet {sheet_name} from {file_path}")
        
        # Find the starting row containing "KINDERGÄRTEN UND KINDERGRUPPEN"
        preview_df = self._get_preview_data(file_path, sheet_name)
        start_row = self._find_section_start(preview_df, self.config['section_markers'][0])
        
        if start_row is None:
            self.logger.warning(f"Could not find section start marker in sheet {sheet_name}")
            return pd.DataFrame()
        
        # Read the data starting from the identified row
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            skiprows=start_row + 2,  # Skip the section header and column headers
            nrows=30,  # Read enough rows to capture all entries
            usecols="A:G"
        )
        
        # Extract data from each section
        data = []
        data.extend(self._extract_verpflegung(df))
        data.extend(self._extract_zusatzleistungen(df))
        
        # Create DataFrame from collected data
        result_df = pd.DataFrame(data)
        
        # Clean up the data
        result_df = result_df.replace({np.nan: None})
        result_df['source_file'] = Path(file_path).stem
        
        self.logger.info(f"Extracted {len(result_df)} rows from sheet {sheet_name}")
        return result_df
    
    def _find_section_start(self, df: pd.DataFrame, marker: str) -> Optional[int]:
        """Find the row index where a section starts."""
        for idx, row in df.iterrows():
            if any(isinstance(val, str) and marker.upper() in str(val).upper() 
                   for val in row.values if pd.notna(val)):
                return idx
        return None
    
    def _extract_section_data(self, df: pd.DataFrame, category: str, section_start_marker: Optional[str] = None, 
                            valid_types: Optional[List[str]] = None, section_end_marker: Optional[str] = None) -> List[Dict]:
        """Generic method to extract data from a section.
        
        Args:
            df: DataFrame containing the data
            category: Category name for the extracted data
            section_start_marker: Optional marker to find the section start
            valid_types: List of valid types for this category
            section_end_marker: Optional marker to find the section end
            
        Returns:
            List of dictionaries containing the extracted data
        """
        data = []
        
        # If we have a section start marker, find the starting index
        start_idx = 0
        if section_start_marker:
            start_indices = df[df.iloc[:, 0] == section_start_marker].index
            if not len(start_indices):
                return data
            start_idx = start_indices[0] + 1
        
        # Process each row
        for idx in range(start_idx, len(df)):
            row = df.iloc[idx]
            entry_type = row.iloc[0]
            
            if pd.isna(entry_type):
                continue
                
            entry_type = entry_type.strip()
            
            # Check for section end if provided
            if section_end_marker and entry_type.startswith(section_end_marker):
                break
                
            # Check if this is a valid type
            if valid_types and entry_type not in valid_types:
                continue
                
            data.append({
                'category': category,
                'type': entry_type,
                'amount': row['Betrag in EUR'] if 'Betrag in EUR' in row.index else row.iloc[2],
                'frequency': row['Anzahl pro Jahr\n(z.B. 12 mal)'] if 'Anzahl pro Jahr\n(z.B. 12 mal)' in row.index else row.iloc[3]
            })
        
        return data
    
    def _extract_verpflegung(self, df: pd.DataFrame) -> List[Dict]:
        """Extract Verpflegung (catering) related entries."""
        verpflegung_types = self.config['verpflegung_structure']['Verpflegung:']
        return self._extract_section_data(
            df, 
            category='Verpflegung',
            valid_types=verpflegung_types
        )
    
    def _extract_zusatzleistungen(self, df: pd.DataFrame) -> List[Dict]:
        """Extract Zusatzleistungen (additional services) entries."""
        return self._extract_section_data(
            df,
            category='Zusatzleistungen',
            section_start_marker='Zusatzleistungen (bitte detailliert anführen):',
            section_end_marker='Einmalzahlungen'
        ) 