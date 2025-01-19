import pandas as pd
from helpers import read_specific_data, write_to_db, get_engine, get_sheet_name, get_meta_info, get_letter
from sql_data_types import sql_types_verteilungsschluessel
import configparser
import os
import re


def process_file(file_name, sheet_name):
    usecols = "J, K, L, M, N, O,P,Q"
    start_row = 1
    skip_from = start_row = 1
    end_row = start_row + 50

    file_path, skip_to, amount_of_rows = get_meta_info(file_name, start_row, end_row)
    sheet_name = get_sheet_name(file_name, "verteilungsschluessel")
    df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=range(skip_from, skip_to), nrows=amount_of_rows)


    pattern = r'.*Kindergarten/\nKindergruppe.*'
    # Find the indices of the first cell that matches the regex pattern
    for row_index, row in enumerate(df.values):
        for col_index, cell in enumerate(row):
            match = re.search(pattern, str(cell))
            if match:
                matched_row_index = row_index  # Row index
                matched_col_index = col_index  # Column index
                break

    from_col_index = matched_col_index
    to_col_index  = from_col_index+4


    # Create a list of column letters using get_letter(x) function
    columns = [get_letter(x) for x in range(from_col_index, to_col_index)]

    # Join the list of column letters into a comma-separated string
    usecols = ", ".join(columns)

    start_row = matched_row_index+2
    end_row = start_row + 3
    ebene_1 = "C. VERTEILUNGSSCHLÜSSEL KINDERGARTEN/KINDERGRUPPE UND HORT 1)"
    ebene_2 = ''
    ebene_3 = ''
    table_name = 'Deckblatt_Verteilungsschluessel'

    # ggf. direkt aus obigem df rauslesen
    df = read_specific_data(file_name, start_row, end_row, sheet_name, usecols, ebene_1, ebene_2, ebene_3)
    df.columns.values[0] = 'Jahr'
    df = df[df['Jahr'] != 0]
    df.columns.values[1] = 'Kindergarten_Kindergruppe'
    df.columns.values[2] = 'TBD'
    df.columns.values[3] = 'Hort'
    df.reset_index(drop=True, inplace=True)
    df.columns = ['Jahr', 'Kindergarten_Kindergruppe', 'TBD', 'Hort', 'Ebene_1',
        'Ebene_2', 'Ebene_3', 'Quelle']
    df = df.drop(columns='TBD')

    df = pd.melt(df, id_vars=["Jahr", "Ebene_1", "ebene_3", "Quelle"], 
                        value_vars=["Kindergarten_Kindergruppe", "Hort"], 
                        var_name="Ebene_2", value_name="Verteilungsschlüssel")
   
    write_to_db(table_name=table_name, df=df, sheet_name=sheet_name, engine=get_engine(), add_metadata_to_table=True, file_name=file_name, sql_types=sql_types_verteilungsschluessel)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('input_files.ini')

    script_basename = os.path.basename(__file__)  # Gets the filename of the script
    script_key = script_basename.split('_', 1)[1].split('.')[0]  # Splits on the first underscore and dot

    # Iterate over all sections in the config
    for section in config.sections():
        if script_key in config[section]:  # Check if the script key exists in the section
            file_name = section
            sheet_name = config[section][script_key]
            process_file(file_name, sheet_name)
