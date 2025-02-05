"""
Concrete implementation of Excel extractor for Elternbeiträge (parent contributions) data.
"""

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional, Tuple
import numpy as np

from .base_extractor import BaseExcelExtractor

class ElternbeitraegeExtractor(BaseExcelExtractor):
    def extract_data(self, file_path: str | Path) -> pd.DataFrame:
        """
        Extract parent contribution data from Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            pd.DataFrame: Extracted and transformed data
            
        Raises:
            ValueError: If required sections are not found in config
        """
        self.logger.info(f"Starting data extraction from {file_path}")
        
        # Validate config structure
        required_sections = ['verpflegung_structure', 'zusatzleistungen_structure']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required section '{section}' in config")
        
        # Find the correct sheet
        xl = pd.ExcelFile(file_path)
        sheet_name = self._find_matching_sheet(xl, self.config['sheet_patterns'])
        
        # Find the starting row containing "KINDERGÄRTEN UND KINDERGRUPPEN"
        preview_df = self._get_preview_data(file_path, sheet_name)
        start_row = self._find_section_start(preview_df, self.config['section_markers'][0])
        
        if start_row is None:
            raise ValueError(f"Could not find section start marker in {file_path}")
        
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
        
        self.logger.info(f"Extracted {len(result_df)} rows of data")
        return result_df
    
    def _find_section_start(self, df: pd.DataFrame, marker: str) -> Optional[int]:
        """Find the row index where a section starts."""
        for idx, row in df.iterrows():
            if any(isinstance(val, str) and marker.upper() in str(val).upper() 
                   for val in row.values if pd.notna(val)):
                return idx
        return None
    
    def _extract_verpflegung(self, df: pd.DataFrame) -> List[Dict]:
        """Extract Verpflegung (catering) related entries."""
        data = []
        verpflegung_types = self.config['verpflegung_structure']['Verpflegung']
        
        for _, row in df.iterrows():
            category = row.iloc[0]
            if pd.isna(category):
                continue
                
            if category.strip() in verpflegung_types:
                data.append({
                    'category': 'Verpflegung',
                    'type': category.strip(),
                    'amount': row['Betrag in EUR'] if 'Betrag in EUR' in row.index else None,
                    'frequency': row['Anzahl pro Jahr\n(z.B. 12 mal)'] 
                        if 'Anzahl pro Jahr\n(z.B. 12 mal)' in row.index else None
                })
        
        return data
    
    def _extract_zusatzleistungen(self, df: pd.DataFrame) -> List[Dict]:
        """Extract Zusatzleistungen (additional services) entries."""
        data = []
        
        # Find where Zusatzleistungen section starts
        zusatz_start = df[df.iloc[:, 0] == 'Zusatzleistungen'].index
        if not len(zusatz_start):
            return data
            
        zusatz_idx = zusatz_start[0]
        for idx in range(zusatz_idx + 1, len(df)):
            row = df.iloc[idx]
            if pd.isna(row.iloc[0]):
                continue
                
            if row.iloc[0].startswith('Einmalzahlungen'):  # Stop at Einmalzahlungen
                break
                
            data.append({
                'category': 'Zusatzleistungen',
                'type': row.iloc[0].strip(),
                'amount': row.iloc[2] if not pd.isna(row.iloc[2]) else None,
                'frequency': row.iloc[3] if not pd.isna(row.iloc[3]) else None
            })
        
        return data 