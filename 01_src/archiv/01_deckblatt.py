import pandas as pd
from helpers import read_specific_data, write_to_db
import configparser
import os


def process_deckblatt(file_name, sheet_name, start_row, end_row, ebene_1, ebene_2, table_name, ebene_3=""):
    all_df = pd.DataFrame()
    for year in [2020, 2021]:
        usecols = "C,D,H" if year == 2020 else "C,E,H"
        
        # Add debug print to see what columns are actually being read
        df = read_specific_data(file_name, start_row, end_row, sheet_name, usecols, ebene_1, ebene_2, ebene_3)
        print(f"Columns in DataFrame: {df.columns.tolist()}")  # Debug print
        
        # Make sure we're only selecting the columns we want
        if len(df.columns) > 3:
            df = df.iloc[:, [0, 1, 2]]  # Select only the first three columns
            
        # Now rename the columns
        df.columns = ["ebene_3", "kennzahl", "abweichung_zum_vorjahr"]
        df['jahr'] = year
        all_df = pd.concat([all_df, df], ignore_index=True)
    write_to_db(table_name, all_df, file_name, sheet_name)

def process_file(file_name, sheet_name):
    sheet_definitions = [
        ("A. KINDERGÄRTEN UND KINDERGRUPPEN", 16, 17, "Anzahl der Standorte (Stichtag 31.12.2021)", "Deckblatt_Standorte"),
        ("B. HORT", 35, 36, "Anzahl der Standorte (Stichtag 31.12.2021)", "Deckblatt_Standorte"),
        ("A. KINDERGÄRTEN UND KINDERGRUPPEN", 18, 21, "Kinderanzahl alle Standorte (Jahresdurchschnitt)", "Deckblatt_Kinderanzahl"),
        ("B. HORT", 37, 38, "Kinderanzahl alle Standorte (Jahresdurchschnitt)", "Deckblatt_Kinderanzahl"),
        ("A. KINDERGÄRTEN UND KINDERGRUPPEN", 22, 31, "Gruppenanzahl aller Standorte (Stichtag 31.12.2021)", "Deckblatt_Gruppen"),
        ("B. HORT", 39, 44, "Gruppenanzahl aller Standorte (Stichtag 31.12.2021)", "Deckblatt_Gruppen")
    ]

    for ebene_1, start_row, end_row, ebene_2, db_target_table_name in sheet_definitions:    
        process_deckblatt(file_name, sheet_name, start_row, end_row, ebene_1, ebene_2, db_target_table_name)

if __name__ == "__main__":
   
    config = configparser.ConfigParser()
    config.read('input_files.ini')

    script_basename = os.path.basename(__file__)  # Gets the filename of the script
    script_key = script_basename.split('_', 1)[1].split('.')[0]  # Splits on the first underscore and dot

    # Iterate over all sections in the config
    found_sections = config.sections()
    for section in found_sections:
        print(found_sections)
        if script_key in config[section]:  # Check if the script key exists in the section
            file_name = section
            sheet_name = config[section][script_key]
            process_file(file_name, sheet_name)
        else:
            print(f"Warning: Script key '{script_key}' not found in section '{section}' of input_files.ini. Skipping.")
