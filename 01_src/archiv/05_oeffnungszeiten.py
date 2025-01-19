import pandas as pd
import logging.config
from helpers import write_to_db, get_meta_info, read_specific_data, get_letter, columns_with_value_contains, write_to_db
import configparser
import os
from sql_data_types import sql_types_metadata


# Configure logging using the external configuration file
logging.config.fileConfig('logging_config.ini')

# Use the logger in your code
logger = logging.getLogger("mainLogger")


def process_file(file_name, sheet_name):
    file_path, _, _ = get_meta_info(file_name, 1, 1)
    # read the whole sheet
    df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1000)

    logger.info('Read excel: {}'.format(file_path))
    # use simple regex to find the starting start_row. The "key_word" indicates the starting point. 
    key_word = 'D. ÖFFNUNGSZEITEN'

    # finde im Sheet jene Zeile, die das key_word ('D. ÖFFNUNGSZEITEN') enthält
    mask = df.apply(lambda row: row.astype(str).str.contains(key_word, case=False).any(), axis=1)
    rows_with_key_word = df[mask]

    # Daten sollen ab dieser Zeile gelesen werden
    start_row = rows_with_key_word.index[0]
    end_row = start_row + 14

    """
    find the starting columns, returns column name (based on dataframe, and column where the data is located)
    should return only one column. The dataframe is read in such a way that the keyword could be anywhere i
    in the dataframe (because the excels all look so differnt). Thus the function searches across the whole datframe to find the column that contains the keyword.

    remember, indexing starts at 0
    """
    # columns_containing = columns_with_value_contains(df, key_word)
    start_col = columns_with_value_contains(df, key_word)[0][1]
    end_col = columns_with_value_contains(rows_with_key_word, "Stunden")[0][1]+2
    

    selected_data = df.iloc[start_row:end_row, start_col:end_col] 

    # drop  with nan
    # df_cleaned = selected_data.dropna(axis=1, how='all')
    df_cleaned = selected_data.dropna(how='all')

    # rename columns
    df_cleaned.columns = df_cleaned.iloc[0]
    df_cleaned = df_cleaned.drop(df_cleaned.index[0])
    df_cleaned.reset_index(drop=True, inplace=True)

    # drop columns with nan
    df_cleaned = df_cleaned.dropna(axis=1, how='all')

    # set defaul column names
    df_cleaned.columns.values[0] = "Name_Eintrag"
    df_cleaned.columns.values[1] = "Eintrag"
    df_cleaned['Erlaeuterung'] = "n. a."
    df_cleaned['ebene_1'] = "D. ÖFFNUNGSZEITEN 3)"
    df_cleaned['ebene_2'] = "n. a."
    df_cleaned['ebene_3'] = "n. a."
    df_cleaned['Quelle'] = f"{file_name}#{sheet_name}"
    df_cleaned = df_cleaned.fillna(0)
    """ 
    # @todo: loop necessary? 
    for _, idx, in columns_containing:
        use_col = get_letter(idx+2) + ", " + end_col # goes from get_letter(idx+2) to end_col
        data = read_specific_data(file_name, start_row, end_row, sheet_name, use_col, ebene_1, ebene_2, ebene_3)
        data = data.iloc[1:]
        alle_zeiten = pd.concat([alle_zeiten, data], ignore_index=True)

    alle_zeiten.columns.values[0] = "Name_Eintrag"
    alle_zeiten.columns.values[1] = "Eintrag" # Anzahl der Stunden pro Woche
    alle_zeiten['Erlaeuterung'] = erlaeuterung """

    table_name = 'Metadaten'
    # alle_zeiten['Eintrag'] = alle_zeiten['Eintrag'].astype('object')
    write_to_db(table_name, df_cleaned, file_name, sql_types = sql_types_metadata, sheet_name=sheet_name)

if __name__ == "__main__":
   
    config = configparser.ConfigParser()
    config.read('input_files.ini', encoding='utf-8')
    script_basename = os.path.basename(__file__)  # Gets the filename of the script
    script_key = script_basename.split('_', 1)[1].split('.')[0]  # Splits on the first underscore and dot

    # Iterate over all sections in the config
    for section in config.sections():
        if script_key in config[section]:  # Check if the script key exists in the section
            file_name = section
            sheet_name = config[section][script_key]
            try:
                process_file(file_name, sheet_name)
            except Exception as e:
                print(f"Error processing file '{file_name}' and sheet '{sheet_name}'. Error: {e}")
                break  # Stop the whole execution if an error occurs
        else:
            print(f"Warning: Script key '{script_key}' not found in section '{section}' of input_files.ini. Skipping.")
