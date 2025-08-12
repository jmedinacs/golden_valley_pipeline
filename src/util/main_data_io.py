'''
Created on Aug 9, 2025

@author: jarpy
'''

import pandas as pd 
import os

def load_raw_data(client_name="missing_client",filename="missing_filename"):
    """
    Loads a raw CSV file from the '../../data/raw/' directory.

    Parameters:
        filename (str): Name of the raw CSV file (no extension)

    Returns:
        pd.DataFrame: Raw dataset

    Raises:
        FileNotFoundError: If the directory is missing
    """
    base_path = "../../data"
    
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"{filename}.csv client base path not found!")
    
    client_path = os.path.join(base_path, client_name)
    if not os.path.exists(client_path):
        raise FileNotFoundError(f"{client_path} not found!")
    
    file_path = os.path.join(client_path,f"data/raw/{filename}.csv")
    df = pd.read_csv(file_path)
    
    print(f"{filename}.csv loaded")
    return df
    
    