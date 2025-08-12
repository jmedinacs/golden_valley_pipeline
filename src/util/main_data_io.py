'''
main_data_io.py

Handles core file input/output operations for the GV ETL Pipeline.

This module centralizes all loading and saving of:
- Raw CSV timecard data from the client’s `data/raw` folder.
- Processed CSV outputs into the client’s `data/processed` folder.

All paths are relative to the repository’s structure to keep the code portable.
The functions are designed to fail loudly (raise exceptions) if expected directories are missing,
helping ensure that directory structure issues are caught early in the pipeline.

Created on Aug 9, 2025
@author: John Medina
'''

import pandas as pd
import os


def load_raw_data(client_name: str = "missing_client", filename: str = "missing_filename") -> pd.DataFrame:
    """
    Loads a raw CSV file from the client's `data/raw` folder.

    Parameters:
        client_name (str): Name of the client folder inside `../../data/`.
        filename (str): Name of the CSV file (without extension) to load.

    Returns:
        pd.DataFrame: Raw dataset as a Pandas DataFrame.

    Raises:
        FileNotFoundError: If the base path, client folder, or target file is missing.
    """
    # Base path for all client data
    base_path = "../../data"
    
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Client base path not found: {base_path}")
    
    # Path to the specific client's folder
    client_path = os.path.join(base_path, client_name)
    if not os.path.exists(client_path):
        raise FileNotFoundError(f"Client folder not found: {client_path}")
    
    # Full path to the raw CSV file
    file_path = os.path.join(client_path, f"data/raw/{filename}.csv")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Raw data file not found: {file_path}")
    
    # Load CSV into DataFrame
    df = pd.read_csv(file_path)
    print(f"{filename}.csv loaded from raw folder.")
    return df


def save_processed_raw_data(df: pd.DataFrame, client_name: str = "missing_client", filename: str = "missing_filename") -> None:
    """
    Saves a cleaned or processed DataFrame to the client's `data/processed` folder.

    Parameters:
        df (pd.DataFrame): DataFrame to save.
        client_name (str): Name of the client folder inside `../../data/`.
        filename (str): Base name for the saved CSV (without `_cleaned` or extension).

    Raises:
        FileNotFoundError: If the processed folder does not exist.
    """
    # Base path for all client data
    base_path = "../../data"
    client_path = os.path.join(base_path, client_name)
    
    # Expected processed folder path
    processed_path = os.path.join(client_path, "data/processed")
    if not os.path.exists(processed_path):
        raise FileNotFoundError(f"Processed folder missing: {processed_path}")

    # Full save path
    file_path = os.path.join(processed_path, f"{filename}_cleaned.csv")
    df.to_csv(file_path, index=False)
    print(f"{filename}_cleaned.csv saved to processed folder.")
