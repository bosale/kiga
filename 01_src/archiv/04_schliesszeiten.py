import pandas as pd
import logging.config
from helpers import write_to_db, get_meta_info, read_specific_data, get_letter, columns_with_value_contains, write_to_db
import configparser
import os
# Configure logging using the external configuration file
logging.config.fileConfig('logging_config.ini')

# Use the logger in your code
logger = logging.getLogger("mainLogger")


def process_file(file_name, sheet_name):
    file_path, _, _ = get_meta_info(file_name, 1, 1)
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1000)
        logger.info('Read excel: {}'.format(file_path))
    except ValueError as e:
        logger.info(e)
    logger.info('dynamisch die Reihen/Spalten finden, die die Schliesszeiten enthalten, basierend auf dem Spaltennamen "Kindergartenjahr..."')
    

    """Annahme: die Schliesszeiten tauchen in diesem Format dreimal vor:
            - Es gibt die Monate September bis August, danach "Gesamt"
            - Die Werte dafür stehen in der Spalte gleich neben den Monanten bzw. "Gesamt"
            - Zwischen September und "Kindergartenjahr 2020/2021" ist genau eine leere Zeile
    
        Kindergartenjahr 2020/2021	
        
        September	
        Oktober	
        November	
        Dezember	5.0
        Jänner	2.0
        Februar	
        März	
        April	
        Mai	1.0
        Juni	1.0
        Juli	5.0
        August	5.0
        Gesamt	19.0
    """

    key_word = 'September'

    # wenn key_word mehr als dreimal vorkommt --> Fehler
    start_cols = columns_with_value_contains(df, key_word)
    if len(start_cols) != 3:
        raise ValueError("Zu viele Schliesszeiten gefunden.")
    

    # die Reihen finden, wo September steht
    mask = df.apply(lambda row: row.astype(str).str.contains(key_word, case=False).any(), axis=1)
    rows_with_key_word = df[mask]

    # extrahiert werden alle Werte von September bis inkl. Gesamt
    start_row = rows_with_key_word.index[0] - 2 # -2 damit Überschrift z. B. Kindergartenjahr 2020/2021	 dabei ist
    end_row = start_row + 13


    # jeden Schliesszeiten-Block extrahieren
    alle_zeiten = pd.DataFrame()
    for _, start_col in start_cols:
        end_col = start_col + 2
        selected_data = df.iloc[start_row:end_row, start_col:end_col].reset_index(drop=True)

        selected_data = selected_data.dropna(how='all')
        selected_data.columns.values[0] = "Name_Eintrag"
        selected_data.columns.values[1] = "Eintrag"

        selected_data['Erlaeuterung'] = "n. a."
        selected_data['ebene_1'] = "C. SCHLIESSZEITEN 2)"
        selected_data['ebene_2'] = selected_data.iloc[0,0] # 0,0 ist die Überschrift
        selected_data['ebene_3'] = "n. a."
        selected_data['Quelle'] = f"{file_name}#{sheet_name}"
        selected_data = selected_data.iloc[1:]
        selected_data = selected_data.fillna(0)
        alle_zeiten = pd.concat([alle_zeiten, selected_data], ignore_index=True) 


    table_name = 'Metadaten'
    alle_zeiten['Eintrag'] = alle_zeiten['Eintrag'].astype('object')
    
    write_to_db(table_name, alle_zeiten, file_name, sheet_name)

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
        else:
            print(f"Warning: Script key '{script_key}' not found in section '{section}' of input_files.ini. Skipping.")
