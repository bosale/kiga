import configparser
from helpers import read_specific_data, write_to_db
import os
import pandas as pd
from helpers import get_meta_info, read_specific_data, fill_down, clean_ebene_4, starts_with_roman_numeral, write_to_db
import logging.config
import warnings

warnings.filterwarnings("ignore", category=pd.core.generic.SettingWithCopyWarning)

# Configure logging using the external configuration file
logging.config.fileConfig('logging_config.ini')

# Use the logger in your code
logger = logging.getLogger("mainLogger")


def process_file(file_name, sheet_name, marker_a, marker_b):
    skip_to = 10
    file_path, _, _ = get_meta_info(file_name, 1, 1)
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, usecols="C",  skiprows=range(1, skip_to), nrows=1000)
        logger.info('Read excel.')
    except ValueError as e:
        logger.error(e)
        # raise


    # marker_a = 'A. AUFWAND'
    # marker_b = 'B. ERTRÄGE'
        
    logger.info('Iterate through the DataFrame and set ebene_1 based on the markers')
    current_ebene_1 = ''
    for idx, row in df.iterrows():
        value = row[0]
        if value == marker_a:
            current_ebene_1 = marker_a
        elif value == marker_b:
            current_ebene_1 = marker_b
        df.at[idx, 'Ebene_1'] = current_ebene_1


    logger.info('reihen rausfiltern basierend auf Nummerierung')
    desired_rows = df[df[df.columns[0]].apply(lambda x: isinstance(x, str) and starts_with_roman_numeral(x))]
    row_numbers = desired_rows.index +  skip_to + 1
    desired_rows.reset_index(inplace=True, drop=True)
    desired_rows['start_row'] = row_numbers
    desired_rows['end_row'] = ''
    desired_rows.columns.values[0] = "Ebene_2"

    desired_rows['end_row'] = desired_rows['start_row'].shift(-1)
    desired_rows['end_row'] = desired_rows['end_row']-1
    desired_rows.fillna(159, inplace=True) # letzte Reihe stopp bei 158


    all_df = pd.DataFrame()
    ebene_3 = ''

    for idx, row in desired_rows.iterrows():
        ebene_1 = row['Ebene_1']
        ebene_2 = row['Ebene_2']
        start_row = row['start_row']
        end_row =  row['end_row']
        all_years = pd.DataFrame()

        for year in [2020, 2021]:
            usecols = "C,D,F, G" if year == 2020 else "C,E,F, G"
            df_ebene_2 = read_specific_data(file_name, start_row, end_row, sheet_name, usecols, ebene_1, ebene_2, ebene_3)

        

            df_ebene_2.rename(columns={df_ebene_2.columns[0]: 'Ebene_4'}, inplace=True)
            df_ebene_2['Ebene_3'] = df_ebene_2['Ebene_4']
            
            # "bug" siehe Z. 99 / 100 in Excel
            df_ebene_2 = df_ebene_2[df_ebene_2['Ebene_3'] != 0]
            df_ebene_2.reset_index(drop=True, inplace=True)

            df_ebene_2['Jahr'] = year
            df_ebene_2['Typ'] = "TBD"
            
            df_ebene_2.columns.values[2] = "Abweichung"
            df_ebene_2.columns.values[1] = "kennzahl"
            df_ebene_2.columns.values[3] = "Kommentar"
            all_years = pd.concat([all_years, df_ebene_2], ignore_index=True)

        df = fill_down(all_years)
        df = clean_ebene_4(df)
        all_df = pd.concat([all_df, df], ignore_index=True)
        
    table_name = 'Betraege'
    write_to_db(table_name, all_df, file_name, sheet_name)

if __name__ == "__main__":
   
    config = configparser.ConfigParser()
    config.read('input_files.ini', encoding='utf-8')

    script_basename = os.path.basename(__file__)  # Gets the filename of this script
    script_key = script_basename.split('_', 1)[1].split('.')[0]  # Splits on the first underscore and dot

    # Iterate over all sections in the config
    # section, z. B. JA 2021 MG_2.xls
    # script_key = 06_B_KIGA
    for section in config.sections():
        if script_key in config[section]:  # Check if the script key exists in the section
            file_name = section
            sheet_names = config[section][script_key].split(',')
            marker_a = config[section]["marker_a"]
            marker_b = config[section]["marker_b"]
            for sheet_name in sheet_names:
                print(sheet_name)
                try:
                    process_file(file_name, sheet_name, marker_a, marker_b)
                except Exception as e:
                    print(f"Error processing file '{file_name}' and sheet '{sheet_name}'. Error: {e}")
                    break  # Stop the whole execution if an error occurs
        else:
            print(f"Warning: Script key '{script_key}' not found in section '{section}' of input_files.ini. Skipping.")
