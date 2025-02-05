"""
Handles data extraction from kindergarten Excel files.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional

from .structures import SECTION_A_STRUCTURE, SECTION_B_STRUCTURE

class KindergartenDataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.xl = pd.ExcelFile(file_path)

    def _extract_section(self, structure: Dict, skiprows: int, nrows: Optional[int] = None) -> pd.DataFrame:
        """Generic method to extract data from a section with given structure."""
        df = pd.read_excel(
            self.file_path,
            sheet_name=self.xl.sheet_names[1],
            skiprows=skiprows,
            usecols="C:E",
            nrows=nrows
        )

        data = []
        current_level_1 = None

        for _, row in df.iterrows():
            category = row.iloc[0]

            if pd.isna(category):
                continue

            if category in structure.keys():
                current_level_1 = category
                continue

            if (current_level_1 and 
                isinstance(category, str) and 
                category in structure[current_level_1]):

                data.append({
                    'level_1': current_level_1,
                    'level_2': category,
                    'value_2022': row.iloc[1] if not pd.isna(row.iloc[1]) else None,
                    'value_2023': row.iloc[2] if not pd.isna(row.iloc[2]) else None
                })

        result_df = pd.DataFrame(data)
        result_df = result_df.replace({np.nan: None})
        result_df['source_file'] = Path(self.file_path).stem

        return result_df

    def extract_section_a(self) -> pd.DataFrame:
        """Extract data from Section A of the kindergarten Excel file."""
        df = self._extract_section(SECTION_A_STRUCTURE, skiprows=13)
        df['section'] = 'A'
        return df

    def extract_section_b(self) -> pd.DataFrame:
        """Extract data from Section B (Hort) of the kindergarten Excel file."""
        df = self._extract_section(SECTION_B_STRUCTURE, skiprows=33, nrows=15)
        df['section'] = 'B'
        return df

    def extract_all_sections(self) -> pd.DataFrame:
        """Extract data from both sections and combine them."""
        df_a = self.extract_section_a()
        df_b = self.extract_section_b()
        return pd.concat([df_a, df_b], ignore_index=True) 