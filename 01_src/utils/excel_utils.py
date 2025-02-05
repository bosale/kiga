import pandas as pd
import os
import logging
from pathlib import Path
import yaml
from fuzzywuzzy import fuzz

def find_sheet_with_content(file_path, search_text, nrows=500):
    """
    Find the first sheet in an Excel file that contains the specified text.
    
    Args:
        file_path (str): Path to the Excel file
        search_text (str): Text to search for in the sheet
        nrows (int): Number of rows to preview in each sheet (default: 50)
    
    Returns:
        str: Name of the sheet containing the text, or None if not found
    """
    xl = pd.ExcelFile(file_path)
    
    for sheet_name in xl.sheet_names:
        # Skip the INFORMATION sheet
        if sheet_name.upper() == "INFORMATION":
            continue
            
        # Read first few rows to check for the search text
        preview_df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            nrows=nrows
        )
        # Convert values to string and check if search text exists
        if any(search_text in str(val).upper() 
               for val in preview_df.values.flatten() 
               if pd.notna(val)):
            return sheet_name
    
    return None

def process_multiple_files(
    directory_path, 
    extraction_function,
    file_pattern="*.xlsx", 
    checkpoint_file="processed_files.json", 
    debug_limit=None,
    process_type="generic",
    default_columns=None
):
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
    
    Args:
        directory_path: Path to directory containing files to process
        extraction_function: Function to extract data from each file
        file_pattern: Pattern to match files (default: "*.xlsx")
        checkpoint_file: Path to checkpoint file (default: "processed_files.json")
        debug_limit: Limit number of files to process (default: None)
        process_type: Type of processing for logging (default: "generic")
        default_columns: Default columns for empty DataFrame (default: None)
    """
    import glob
    from .checkpoint_utils import get_processed_files, update_checkpoint, handle_problematic_files
    
    # Get list of all Excel files in the directory
    file_paths = glob.glob(os.path.join(directory_path, file_pattern))
    
    if not file_paths:
        raise FileNotFoundError(f"No Excel files found in {directory_path}")
    
    # Limit files if in debug mode
    if debug_limit is not None:
        file_paths = file_paths[:debug_limit]
        logging.info(f"DEBUG MODE: Processing only {debug_limit} files")
        processed_files = set()
    else:
        processed_files = get_processed_files(checkpoint_file)
    
    # Initialize lists for results and problematic files
    all_results = []
    problematic_files = []
    
    # Process each file
    for file_path in file_paths:
        file_name = Path(file_path).name
        if debug_limit is not None or file_name not in processed_files:
            try:
                df_result = extraction_function(file_path)
                all_results.append(df_result)
                
                logging.info(f"Successfully processed file: {file_name}")
                if debug_limit is None:
                    update_checkpoint(checkpoint_file, file_name)
            except Exception as e:
                error_message = str(e)
                error_type = type(e).__name__
                logging.error(f"Error processing {file_name}: {error_message}")
                problematic_files.append({
                    'file_name': file_name,
                    'error_type': error_type,
                    'error_description': error_message,
                    'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                continue
    
    # Handle problematic files
    handle_problematic_files(problematic_files, directory_path, process_type)
    
    # Combine results
    if not all_results:
        raise ValueError("No files were successfully processed")
    
    combined_df = pd.concat(all_results, ignore_index=True)
    
    # Add validation for empty DataFrame
    if combined_df.empty:
        logging.warning(f"No data was extracted from any {process_type} files")
        return pd.DataFrame(columns=default_columns if default_columns else ['source_file'])
        
    return combined_df 

def extract_section_data(
    df: pd.DataFrame,
    section_identifier: str,
    structure: dict,
    file_path: str | Path,
    logger: logging.Logger,
    header_row_index: int = 8,
    year_2022_col: int | None = None,
    year_2023_col: int | None = None,
    comment_col: int | None = None
) -> pd.DataFrame:
    """
    Extract data for a specific section (Personnel or Material expenses) from DataFrame.
    
    Args:
        df: DataFrame containing the data
        section_identifier: Section identifier (e.g., 'I.' for Personnel, 'II.' for Material)
        structure: Structure dictionary from YAML config
        file_path: Path to the source file
        logger: Logger instance
        header_row_index: Index of the header row (default: 8)
        year_2022_col: Column index for year 2022 (default: None)
        year_2023_col: Column index for year 2023 (default: None)
        comment_col: Column index for comments (default: None)
        
    Returns:
        pd.DataFrame: Extracted data
    """
    # If year columns are not provided, find them in the header row
    if any(col is None for col in [year_2022_col, year_2023_col, comment_col]):
        # First try to find the actual header row by looking for year columns
        if header_row_index == 8:  # Only if using default
            for idx in range(len(df)):
                row = df.iloc[idx]
                year_found = False
                for col in range(len(row)):
                    cell_value = str(row[col]).strip() if pd.notna(row[col]) else ''
                    if 'abrechnung 2022' in cell_value.lower() or 'abrechnung 2023' in cell_value.lower():
                        header_row_index = idx
                        year_found = True
                        break
                if year_found:
                    break
            logger.debug(f"Found header row at index: {header_row_index}")
        
        header_row = df.iloc[header_row_index]
        
        if year_2022_col is None or year_2023_col is None or comment_col is None:
            for col in range(len(header_row)):
                cell_value = str(header_row[col]).strip() if pd.notna(header_row[col]) else ''
                if year_2022_col is None and ('2022' in cell_value or 'abrechnung 2022' in cell_value.lower()):
                    year_2022_col = col
                elif year_2023_col is None and ('2023' in cell_value or 'abrechnung 2023' in cell_value.lower()):
                    year_2023_col = col
                elif comment_col is None and ('kommentar' in cell_value.lower() or 'zusatzinformation' in cell_value.lower()):
                    comment_col = col

        if any([year_2022_col is None, year_2023_col is None, comment_col is None]):
            logger.warning("Using default column indices as not all columns were found in header")
            year_2022_col = 3 if year_2022_col is None else year_2022_col
            year_2023_col = 4 if year_2023_col is None else year_2023_col
            comment_col = 6 if comment_col is None else comment_col

    logger.debug(f"Using columns - 2022: {year_2022_col}, 2023: {year_2023_col}, comment: {comment_col}")

    # Find the start of the section
    start_row = None
    section_id = structure.get('section_id', '')  # Get section ID from structure
    logger.debug(f"Looking for section with ID: {section_id}")
    
    # Look for the section header through the entire DataFrame
    for idx in range(len(df)):
        row = df.iloc[idx]
        for col in range(len(row)):
            cell_value = str(row[col]).strip() if pd.notna(row[col]) else ''
            # Check for various forms of the section identifier
            if (section_id in cell_value or 
                f'{section_identifier}.' in cell_value or 
                section_identifier.strip() in cell_value):
                start_row = idx
                logger.debug(f"Found section start at row {idx}")
                break
        if start_row is not None:
            break
    
    if start_row is None:
        logger.error(f"Section {section_identifier} not found in file")
        logger.debug("Available sections in structure:")
        logger.debug(structure.keys())
        logger.debug("First 20 rows of data:")
        logger.debug(df.head(20).to_string())
        raise ValueError(f"Could not find section {section_identifier}")
    
    # Initialize data dictionary
    data = {
        'source_file': Path(file_path).name,
        'category': {},
        'subcategory': {},
        'subcategory_desc': {},
        'detail': {},
        'year_2022': {},
        'year_2023': {},
        'comments': {}
    }
    
    # Process each category from the structure
    current_category = None
    current_subcategory = None
    current_subcategory_desc = None
    
    for idx in range(start_row, len(df)):
        row = df.iloc[idx]
        
        # Skip empty rows - fixed to properly handle pandas Series
        if row.isna().all() or row.astype(str).str.strip().eq('').all():
            continue
            
        # Get cell values
        desc = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
        val_2022 = row.iloc[year_2022_col] if pd.notna(row.iloc[year_2022_col]) else None
        val_2023 = row.iloc[year_2023_col] if pd.notna(row.iloc[year_2023_col]) else None
        comment = str(row.iloc[comment_col]).strip() if pd.notna(row.iloc[comment_col]) else ''
        
        # Check if this is a category header
        for category in structure['categories'].keys():
            if category in desc:
                current_category = category
                current_subcategory = category
                current_subcategory_desc = structure['categories'][category].get('description', '')
                logger.debug(f"Found category: {category}")
                break
                
        # If we have a current category, check if this is an item
        if current_category:
            items = structure['categories'][current_category].get('items', [])
            for item in items:
                if item in desc:
                    # Found a matching item, store its data
                    data['category'][item] = section_identifier
                    data['subcategory'][item] = current_subcategory
                    data['subcategory_desc'][item] = current_subcategory_desc
                    data['detail'][item] = desc
                    if val_2022 is not None:
                        try:
                            data['year_2022'][item] = float(str(val_2022).replace(',', '.'))
                        except (ValueError, TypeError):
                            data['year_2022'][item] = None
                    if val_2023 is not None:
                        try:
                            data['year_2023'][item] = float(str(val_2023).replace(',', '.'))
                        except (ValueError, TypeError):
                            data['year_2023'][item] = None
                    data['comments'][item] = comment
                    logger.debug(f"Found item: {item} with values 2022: {val_2022}, 2023: {val_2023}")
                    break
        
        # Check for end of section (next main section)
        if idx > start_row:
            cell_value = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
            if cell_value.startswith('II.') or 'SACHAUSGABEN' in cell_value:
                break
    
    # Convert to DataFrame
    rows = []
    for label in set(list(data['year_2022'].keys()) + list(data['year_2023'].keys())):
        row = {
            'source_file': data['source_file'],
            'category': data['category'].get(label, ''),
            'subcategory': data['subcategory'].get(label, ''),
            'subcategory_desc': data['subcategory_desc'].get(label, ''),
            'detail': data['detail'].get(label, ''),
            'value_2022': data['year_2022'].get(label),
            'value_2023': data['year_2023'].get(label),
            'comment': data['comments'].get(label, '')
        }
        rows.append(row)
    
    if not rows:
        logger.warning(f"No data was extracted from {file_path}")
        raise ValueError(f"No data extracted from {file_path}")
    
    logger.debug(f"Extracted {len(rows)} rows of data")
    return pd.DataFrame(rows) 

def load_structure(config_file: str) -> dict:
    """
    Load a structure configuration from YAML file.
    
    Args:
        config_file: Name of the YAML config file to load
        
    Returns:
        dict: The structure configuration
    """
    structure_file = Path(__file__).parent.parent / "config" / config_file
    with open(structure_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) 

def extract_balance_data(
    df: pd.DataFrame,
    section_identifier: str,
    structure: dict,
    file_path: str | Path,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Extract balance sheet data (Vermögen/Verbindlichkeiten) from DataFrame.
    
    Args:
        df: DataFrame containing the data
        section_identifier: Section identifier (e.g., 'Vermögen', 'Verbindlichkeiten')
        structure: Structure dictionary from YAML config
        file_path: Path to the source file
        logger: Logger instance
        
    Returns:
        pd.DataFrame: Extracted data
    """
    # Find the section start
    start_row = None
    for idx in range(len(df)):
        cell_value = str(df.iloc[idx, 0]).strip() if pd.notna(df.iloc[idx, 0]) else ''
        if section_identifier in cell_value:
            start_row = idx
            break
    
    if start_row is None:
        raise ValueError(f"Section {section_identifier} not found in file")
        
    # Initialize data collection
    rows = []
    items = structure[section_identifier]['items']
    
    # Process rows until we hit "SUMME" or empty rows
    for idx in range(start_row + 1, len(df)):
        description = str(df.iloc[idx, 0]).strip() if pd.notna(df.iloc[idx, 0]) else ''
        
        # Stop if we hit SUMME
        if 'SUMME' in description.upper():
            break
            
        # Skip empty rows
        if not description:
            continue
            
        # Check if this row matches any item in our structure
        for item in items:
            if description.startswith(item.split('(')[0].strip()):
                try:
                    value_2023_start = df.iloc[idx, 1] if pd.notna(df.iloc[idx, 1]) else None
                    value_2023_end = df.iloc[idx, 2] if pd.notna(df.iloc[idx, 2]) else None
                    change = df.iloc[idx, 3] if pd.notna(df.iloc[idx, 3]) else None
                    
                    rows.append({
                        'source_file': Path(file_path).stem,
                        'category': section_identifier,
                        'item': item,
                        'value_2023_start': value_2023_start,
                        'value_2023_end': value_2023_end,
                        'change': change
                    })
                except Exception as e:
                    logger.warning(f"Error processing row {idx} for item {item}: {e}")
                break
    
    if not rows:
        raise ValueError(f"No data extracted from {file_path}")
        
    return pd.DataFrame(rows) 

def find_sheet_by_cell_value(file_path, search_text, cell="A1", threshold=80):
    """
    Find the first sheet in an Excel file that contains text similar to the specified text in a specific cell.
    
    Args:
        file_path (str): Path to the Excel file
        search_text (str): Text to search for in the cell
        cell (str): Cell reference to check (default: "A1")
        threshold (int): Minimum similarity score (0-100) to consider a match (default: 80)
    
    Returns:
        str: Name of the sheet containing similar text in the specified cell, or None if not found
    """
    xl = pd.ExcelFile(file_path)
    
    # Convert search text to uppercase for consistent comparison
    search_text = str(search_text).upper()
    for sheet_name in xl.sheet_names:
        # Skip the INFORMATION sheet
        if sheet_name.upper() == "INFORMATION":
            continue
            
        # Read just the specific cell
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            cell_value = df.columns[0]
            # Convert to string and compare using fuzzy matching
            if pd.notna(cell_value):
                cell_text = str(cell_value).upper()
                # Use token_set_ratio to handle partial matches and different word orders
                similarity = fuzz.token_set_ratio(search_text, cell_text)
                if similarity >= threshold:
                    return sheet_name
        except Exception:
            continue
    
    return None 

def debug_excel_file(file_path: str | Path, sheet_name: str | None = None, nrows: int = 20, save_csv: bool = True) -> None:
    """
    Debug helper function to print the contents of an Excel file and optionally save as CSV.
    
    Args:
        file_path: Path to the Excel file
        sheet_name: Name of the sheet to read (if None, reads first sheet)
        nrows: Number of rows to display (default: 20)
        save_csv: Whether to save the data as CSV (default: True)
    """
    try:
        # Set up logging
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        
        # Read the Excel file
        logger.info(f"\nReading file: {file_path}")
        
        # If sheet_name is not provided, list available sheets
        xl = pd.ExcelFile(file_path)
        if not sheet_name:
            logger.info(f"Available sheets: {xl.sheet_names}")
            sheet_name = xl.sheet_names[0]
            logger.info(f"Using first sheet: {sheet_name}")
            
        # Read the data
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        logger.info(f"DataFrame shape: {df.shape}")
        
        # Print the first nrows rows
        logger.info(f"\nFirst {nrows} rows:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(df.head(nrows).to_string())
        
        # Print column info
        logger.info("\nColumn information:")
        for col in range(len(df.columns)):
            non_null = df[col].count()
            total = len(df)
            logger.info(f"Column {col}: {non_null}/{total} non-null values")
        
        # Save as CSV if requested
        if save_csv:
            # Create debug directory if it doesn't exist
            debug_dir = Path("debug_output")
            debug_dir.mkdir(exist_ok=True)
            
            # Create CSV filename from Excel filename
            excel_name = Path(file_path).stem
            csv_path = debug_dir / f"{excel_name}_{sheet_name}_debug.csv"
            
            # Save to CSV
            df.to_csv(csv_path, index=False)
            logger.info(f"\nSaved debug CSV to: {csv_path}")
            
    except Exception as e:
        logger.error(f"Error reading Excel file: {str(e)}")
        raise 