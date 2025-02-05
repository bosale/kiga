import logging
from utils.excel_utils import debug_excel_file
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# File to debug
file_path = Path("../02_data/01_input/2023_JAB_A.C.H. Montessori Kinderhaus 08.xlsx")
sheet_name = "NB_KIGA"

# Debug the file
debug_excel_file(file_path, sheet_name) 