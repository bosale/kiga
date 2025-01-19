import pandas as pd
import re
import json
from sqlalchemy import create_engine
import string
import configparser
import logging.config

# Replace the fileConfig line with this logging configuration
logger = logging.getLogger("mainLogger")
logger.setLevel(logging.INFO)

# Create console handler with formatting
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(console_handler)


def get_sheet_name(file_name, key):
    logger.info('Started get_sheet_name for {}'.format(key))
    config = configparser.ConfigParser()
    config.read('input_files.ini', encoding='utf-8')
    if file_name in config:
        
        # Check if 'traegerorganisation' is defined for the file
        if key in config[file_name]:
            sheet_name = config[file_name][key]

            logger.info("Value for {} in file {} is: {}".format(key, file_name, sheet_name))
        else:
            print("get_sheet_name: No 'traegerorganisation' defined for file", file_name)
    else:
        print("File", file_name, "not found in the configuration file.")

    return sheet_name

# Hilfsfunktion, um von Zahl auf Buchstabe zu kommen
def get_letter(index):
    # Adjust the index since Python is zero-based
    adjusted_index = index - 1
    # Make sure the index is within the bounds of the alphabet
    if 0 <= adjusted_index < len(string.ascii_uppercase):
        return string.ascii_uppercase[adjusted_index]
    else:
        raise ValueError("Index is out of bounds. Must be between 1 and 26.")
    

def columns_with_value_contains(df, value):
    # Convert all columns to string and then check if they contain the value
    mask = df.apply(lambda col: col.astype(str).str.contains(value, case=False, na=False))
    # Find columns where the value is present
    columns = mask.any()
    
    # Extracting column names and indices
    return [(col, df.columns.get_loc(col)) for col in df.columns[columns]]




def get_engine():
    # Load settings from the JSON file
    with open('config.json', 'r') as file:
        config = json.load(file)

    # Construct the connection string
    connection_string = f"mssql+pyodbc://@{config['server']}/{config['database']}?driver={config['driver']}&trusted_connection={config['trusted_connection']}"

    # Create engine
    engine = create_engine(connection_string)

    return engine




def process_deckblatt(file_name, start_row, end_row, sheet_name, usecols, year, ebene_1, ebene_2, ebene_3):
    df = read_specific_data(file_name, start_row, end_row, sheet_name, usecols, ebene_1, ebene_2, ebene_3)
    df.drop(['Ebene_3'], axis=1, inplace=True)  # ist immer dynamisch, quasi bug
    df.columns.values[0] = "Ebene_3"
    df.columns.values[1] = "kennzahl"
    df.columns.values[2] = "abweichung_zum_vorjahr"
    df['jahr'] = year
    return df


def add_metadata(df, file_name):
    df['Traegerorganisation'] = get_traegerorganisation(file_name)
    df['Jahr_Abrechnung'] = get_jahr_abrechnung(file_name)

    return df


def write_to_db(table_name, df, file_name, sheet_name, engine=False, add_metadata_to_table=True, schema_name='dbo', sql_types = False):
    logger.info('Schreibe in Datenbank.')
    if not engine:
        engine = get_engine()
    if add_metadata_to_table:
        logger.info('Starting add_metadata_to_table')
        df = add_metadata(df, file_name)
        logger.info('added _metadata_to_table')
    df.to_sql(table_name, schema=schema_name, con=engine, if_exists='append', index=False, dtype=sql_types)

    logger.info("Insert erfolgreich. Tabelle: {}. Dateiname: {}. Blattname: {}.".format(table_name, file_name, sheet_name))


def get_traegerorganisation(file_name):
    # bspw. Kinderbetreuungsverein Märchengarten
    logger.info('Started get_traegerorganisation')
    
    
    start_row = 1
    skip_from = 1
    usecols = "A, B, C, D, E, F, G, H, I, J, K, L"
    end_row = 100
    file_path, skip_to, amount_of_rows = get_meta_info(file_name, start_row, end_row)
    sheet_name = get_sheet_name(file_name, "traegerorganisation")
    logger.info("get_traegerorganisation: Got this sheet_name : {}".format(sheet_name))
    df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=usecols, skiprows=range(skip_from, skip_to), nrows=amount_of_rows)


    pattern = r'.*Name der Trägerorganisation.*'
    # Find the indices of the first cell that matches the regex pattern
    for row_index, row in enumerate(df.values):
        for col_index, cell in enumerate(row):
            match = re.search(pattern, str(cell))
            if match:
                matched_row_index = row_index  # Row index
                matched_col_index = col_index  # Column index
                break


    # Initialize variables to store the result
    next_value = None
    for row_index in range(matched_row_index, df.shape[0]):
        for col_index in range(matched_col_index, df.shape[1]):
            col_index = col_index + 1
            cell_value = df.iat[row_index, col_index]
            if not pd.isna(cell_value):
                # Found the next non-NaN cell
                next_value = cell_value
                break
        if next_value is not None:
            break


    traegerorganisation = next_value
    return traegerorganisation



def get_jahr_abrechnung(file_name, start_row = 1, usecols = "A, B, C, D, E, F"):
    logger.debug('Getting get_jahr_abrechnung')

    end_row = 100
    sheet_name = get_sheet_name(file_name, "jahr_abrechnung")
    start_row = 1
    skip_from = 1
    end_row = 100
    file_path, skip_to, amount_of_rows = get_meta_info(file_name, start_row, end_row)
    # header = None, falls der gesuchte Wert in der erste Zeile stehe
    df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=usecols, skiprows=range(skip_from, skip_to), nrows=amount_of_rows, header=None)
    pattern = r'.*JAHRESABRECHNUNG.*'
    # Find the indices of the first cell that matches the regex pattern
    for row_index, row in enumerate(df.values):
        for col_index, cell in enumerate(row):
            match = re.search(pattern, str(cell))
            if match:
                matched_row_index = row_index  # Row index
                matched_col_index = col_index  # Column index
                break

    text_jahr_abrechnung = df.iat[matched_row_index, matched_col_index]
    # Regular expression to find the first four digit number starting with 20 after 'JAHRESABRECHNUNG'
    pattern = r"JAHRESABRECHNUNG (\b20\d{2}\b)"

    # Search for the pattern in the text
    match = re.search(pattern, text_jahr_abrechnung)

    # Extract the matched number if found
    matched_number = match.group(1) if match else None
    return int(matched_number)


def make_file_safe_name(name):
    # Remove all punctuation and make space a separator
    name = re.sub(r'[^\w\s]', '', name)
    # Replace spaces with underscores for file name
    name = re.sub(r'\s+', '_', name)
    return name


def get_meta_info(file_name,start_row, end_row):
    file_path = r'..\02_data\01_input\{}'.format(file_name)
    skip_to = start_row - 1 
    amount_of_rows = end_row - start_row + 1
    return file_path, skip_to, amount_of_rows


def read_specific_data(file_name, start_row, end_row, sheet_name, usecols, ebene_1, ebene_2, ebene_3):
    """
    kategorie = A. ELTERNBEITRÄGE etc.
    """
    skip_from = 1
    file_path, skip_to, amount_of_rows = get_meta_info(file_name, start_row, end_row)
    logger.debug("file_path: '{}', skip_to: {}, amount_of_rows: {}".format(file_path, skip_to, amount_of_rows))
    df = pd.DataFrame()
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=usecols, skiprows=range(skip_from, skip_to), nrows=amount_of_rows)
        column_names = df.iloc[0]
        df.columns = column_names
        df = df.iloc[1:]
        df = df.reset_index(drop=True)

        df.fillna(0, inplace=True)
        if not ebene_1:
            ebene_1 = "n. a."

        if not ebene_2:
            ebene_2 = "n. a."

        if not ebene_3:
            ebene_3 = "n. a."

        df['Ebene_1'] = ebene_1
        df['Ebene_2'] = ebene_2
        df['Ebene_3'] = ebene_3

        # Setzt die 'Quelle' Spalte des DataFrame 'df'.
        # Quelle soll sein: Dateiname (file_name) und Blattname (sheet_name), getrennt duch #
        df['Quelle'] = f"{file_name}#{sheet_name}"
        
        return df
    except (ValueError, IndexError) as e:
        print(e, file_path)
        return df
    


# Function to check if a string starts with a number followed by a dot
def starts_with_number_dot(s):
    if pd.isna(s):
        return False
    try:
        parts = s.strip().split(' ', 1)
        return parts[0].replace('.', '', 1).isdigit() and parts[0].endswith('.')
    except AttributeError as e:
        print(s)
    

    
def starts_with_roman_numeral(s):
    roman_numerals = ['I.', 'II.', 'III.', 'IV.', 'V.', 'VI.', 'VII.', 'VIII.', 'IX.', 'X.', 'XI.', 'XII.']
    return any(s.startswith(roman) for roman in roman_numerals)


def no_summe_after_roman(s):
    if not pd.isnull(s) and starts_with_roman_numeral(s):
        parts = s.split()
        # Check if 'SUMME' is not in any part after the first part (assuming the Roman numeral is the first part)
        return 'SUMME' not in parts[1:]
    return False


def clean_ebene_4(df):
    counts = df['Ebene_3'].value_counts()
    rows_to_remove = []

    for index, row in df.iterrows():
        if row['Ebene_3'] == row['Ebene_4'] and counts[row['Ebene_4']] > 1:
            rows_to_remove.append(index)

    return df.drop(rows_to_remove)


def fill_down(df):
    previous_value = None
    for index, row in df.iterrows():
        if starts_with_number_dot(row['Ebene_3']):
            previous_value = row['Ebene_3']
        elif previous_value is not None:
            df.at[index, 'Ebene_3'] = previous_value

    return df
