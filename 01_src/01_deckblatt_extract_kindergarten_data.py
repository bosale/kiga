import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path
import json

def extract_section_a(file_path):
    """
    Extract data from Section A of the kindergarten Excel file with hierarchical structure.
    Returns a DataFrame with the extracted data and the filename.
    """
    # Define the exact structure we expect
    structure = {
        "Anzahl der Standorte (Stichtag 31.12.2023)": [
            "..mit Kindergarten und Kindergruppen"
        ],
        "Kinderanzahl alle Standorte (Jahresdurchschnitt)": [
            "Kinder 0 - 6 Jahre",
            "Integrationskindergartengruppe",
            "Kinder mit erhöhtem Förderbedarf (EFB)"
        ],
        "Gruppenanzahl aller Standorte (Stichtag 31.12.2023)": [
            "Kleinkindergruppe (Krippe)",
            "Familiengruppe 0 - 6 Jahre",
            "Familiengruppe 2 - 6 Jahre",
            "Familiengruppe 3 - 10 Jahre",
            "Kindergartengruppe ganztags",
            "Kindergartengruppe halbtags",
            "Kindergruppe",
            "Integrationskleinkindergruppe",
            "Integrationskindergartengruppe",
            "Heilpädagogische Kindergartengruppe"
        ]
    }

    # Read the Excel file
    xl = pd.ExcelFile(file_path)
    df = pd.read_excel(
        file_path,
        sheet_name=xl.sheet_names[1],
        skiprows=13,
        usecols="C:E"
    )
    
    # Initialize lists to store the structured data
    data = []
    current_level_1 = None
    
    # Process each row
    for idx, row in df.iterrows():
        category = row.iloc[0]
        
        # Skip empty rows
        if pd.isna(category):
            continue
            
        # Check if this is a level 1 category
        if category in structure.keys():
            current_level_1 = category
            continue
            
        # Check if this is a valid level 2 category for the current level 1
        if (current_level_1 and 
            isinstance(category, str) and 
            category in structure[current_level_1]):
            
            data.append({
                'level_1': current_level_1,
                'level_2': category,
                'value_2022': row.iloc[1] if not pd.isna(row.iloc[1]) else None,
                'value_2023': row.iloc[2] if not pd.isna(row.iloc[2]) else None
            })
    
    # Create DataFrame from the collected data
    result_df = pd.DataFrame(data)
    
    # Clean up the data - replace NaN with None
    result_df = result_df.replace({np.nan: None})
    
    # Add filename to the DataFrame
    result_df['source_file'] = Path(file_path).stem
    
    return result_df

def extract_section_b(file_path):
    """
    Extract data from Section B (Hort) of the kindergarten Excel file with hierarchical structure.
    Returns a DataFrame with the extracted data and the filename.
    """
    # Define the exact structure we expect for section B
    structure = {
        "Anzahl der Standorte (Stichtag 31.12.2023)": [
            "..mit Hort-, Teilhort- und Hortkindergruppen"
        ],
        "Kinderanzahl alle Standorte (Jahresdurchschnitt)": [
            "schulpflichtige Kinder"
        ],
        "Gruppenanzahl aller Standorte (Stichtag 31.12.2023)": [
            "Teilhortgruppe",
            "Hortgruppe",
            "Hortkindergruppe",
            "Integrationshortgruppe",
            "Heilpädagogische Hortgruppe"
        ]
    }

    # Read the Excel file - adjust skiprows to start from section B
    xl = pd.ExcelFile(file_path)
    df = pd.read_excel(
        file_path,
        sheet_name=xl.sheet_names[1],
        skiprows=33,  # Adjusted to start from section B
        usecols="C:E",
        nrows=15  # Limit rows to section B
    )
    
    # Initialize lists to store the structured data
    data = []
    current_level_1 = None
    
    # Process each row
    for idx, row in df.iterrows():
        category = row.iloc[0]
        
        # Skip empty rows
        if pd.isna(category):
            continue
            
        # Check if this is a level 1 category
        if category in structure.keys():
            current_level_1 = category
            continue
            
        # Check if this is a valid level 2 category for the current level 1
        if (current_level_1 and 
            isinstance(category, str) and 
            category in structure[current_level_1]):
            
            data.append({
                'level_1': current_level_1,
                'level_2': category,
                'value_2022': row.iloc[1] if not pd.isna(row.iloc[1]) else None,
                'value_2023': row.iloc[2] if not pd.isna(row.iloc[2]) else None,
                'section': 'B'  # Mark this as section B data
            })
    
    # Create DataFrame from the collected data
    result_df = pd.DataFrame(data)
    
    # Clean up the data - replace NaN with None
    result_df = result_df.replace({np.nan: None})
    
    # Add filename to the DataFrame
    result_df['source_file'] = Path(file_path).stem
    
    return result_df

def get_processed_files(checkpoint_file):
    """Read the checkpoint file containing already processed files"""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return set(json.load(f))
    return set()

def update_checkpoint(checkpoint_file, processed_file):
    """Update the checkpoint file with newly processed file"""
    processed_files = get_processed_files(checkpoint_file)
    processed_files.add(processed_file)
    with open(checkpoint_file, 'w') as f:
        json.dump(list(processed_files), f)

def process_multiple_files(directory_path, file_pattern="*.xlsx", checkpoint_file="processed_files.json"):
    """
    Process multiple Excel files in the specified directory, with checkpoint support.
    Now processes both sections A and B.
    """
    # Get list of all Excel files in the directory
    file_paths = glob.glob(os.path.join(directory_path, file_pattern))
    
    if not file_paths:
        raise FileNotFoundError(f"No Excel files found in {directory_path}")
    
    # Get already processed files
    processed_files = get_processed_files(checkpoint_file)
    
    # Process each file
    all_results = []
    
    # Process each file
    for file_path in file_paths:
        file_name = Path(file_path).name
        if file_name not in processed_files:
            try:
                # Extract both sections
                df_a = extract_section_a(file_path)
                df_a['section'] = 'A'  # Mark section A data
                df_b = extract_section_b(file_path)
                
                # Combine sections
                all_results.extend([df_a, df_b])
                print(f"Successfully processed new file: {file_name}")
                update_checkpoint(checkpoint_file, file_name)
            except Exception as e:
                print(f"Error processing {file_name}: {str(e)}")
                continue
    
    # Combine all results
    if not all_results:
        raise ValueError("No files were successfully processed")
    
    combined_df = pd.concat(all_results, ignore_index=True)
    return combined_df

def clear_checkpoints(checkpoint_file="processed_files.json"):
    """Clear the checkpoint file to start fresh"""
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        print("Checkpoint file cleared.")

if __name__ == "__main__":
    directory_path = r"C:\Users\bol9002\Documents\kindergarten\02_data\01_input"
    checkpoint_file = os.path.join(os.path.dirname(directory_path), "processed_files.json")
    
    # Uncomment the following line if you want to start fresh
    # clear_checkpoints(checkpoint_file)
    
    try:
        combined_results = process_multiple_files(
            directory_path, 
            checkpoint_file=checkpoint_file
        )
        
        print("\nExtracted Data Summary:")
        print(f"Total files processed: {combined_results['source_file'].nunique()}")
        print(f"Total records: {len(combined_results)}")
        
        # Save to CSV
        output_path = os.path.join(os.path.dirname(directory_path), "kindergarten_section_a_combined.csv")
        combined_results.to_csv(output_path, index=False)
        print(f"\nResults saved to: {output_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}") 