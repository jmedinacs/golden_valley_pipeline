'''
Created on Aug 12, 2025

@author: jarpy
'''

import pandas as pd 
import os 

def report_incomplete_rows_global(df: pd.DataFrame, client_name: str = "missing_client", filename: str = "missing_filename") -> None:
    """
    Saves a CSV report of incomplete or invalid rows for a given client dataset.

    This function is typically called after global quality checks have flagged
    rows with missing or problematic data (e.g., missing clock-in/out, pay info, etc.).
    It saves the report to the appropriate `report/incomplete_rows/` folder inside
    the client’s directory.

    Parameters:
        df (pd.DataFrame): DataFrame containing only the incomplete or flagged rows.
        client_name (str): Name of the client folder (within /data).
        filename (str): Base name of the dataset (without extension).

    Raises:
        FileNotFoundError: If the target folder for saving the report does not exist.
    """
    # Define base and client paths
    base_path = "../../../data"
    client_path = os.path.join(base_path, client_name)

    # Path to the specific report folder for incomplete rows
    missing_rows_path = os.path.join(client_path, "report","incomplete_rows")
    if not os.path.exists(missing_rows_path):
        raise FileNotFoundError(f"incomplete_rows folder missing: {missing_rows_path}")

    # Full path to output CSV file
    file_path = os.path.join(missing_rows_path, f"{filename}_incomplete_rows_report.csv")
    df.to_csv(file_path, index=False)
    print(f"{filename}_incomplete_rows_report.csv saved to incomplete_rows folder.")
