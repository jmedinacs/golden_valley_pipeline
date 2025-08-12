'''
Created on Aug 9, 2025

@author: jarpy
'''

import util.main_data_io as mainIO
import pandas as pd 
from pandas import StringDtype

MAPPINGS = {
    # keys must be post-clean (lowercase + underscores, slashes replaced)
    "employment_status": {
        "fulltime": "full_time",
        "f_t": "full_time",      
        "pt": "part_time",
        "parttime": "part_time",
    },
    "exempt_status": {
        "nonexempt": "non_exempt",
        "non-exempt": "non_exempt",
        "n_ex": "non_exempt",    
        "ne": "non_exempt",
    },
}


def inspect_data(df):
    """ """
    print(df.info())
    print("\nNumber of null entries per column\n")
    print(df.isnull().sum())
    
def inspect_text_values(df, text_cols):
    for col in text_cols:
        print(f"\n--- {col} ---")
        print(df[col].value_counts(dropna=False))
        
def standardize_text_columns(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """
    Clean text columns, then apply post-clean mappings.
    Cleaning: strip -> lower -> replace -, space, / with _
    """
    df = df.copy()
    for col, map_dict in mappings.items():
        if col in df.columns:
            # keep real missing as <NA>, not "nan"
            s = df[col].astype(StringDtype())

            s = (
                s.str.strip()
                 .str.lower()
                 .str.replace("-", "_", regex=False)
                 .str.replace(" ", "_", regex=False)
                 .str.replace("/", "_", regex=False)
            )
            if map_dict:
                s = s.replace(map_dict)  # post-clean mappings
            df[col] = s
    return df
    


def initial_inspection(client_name="missing_client", filename="missing_filename"):
    # 1) Load
    df = mainIO.load_raw_data(client_name, filename)

    # 2) Inspect
    inspect_data(df)
    text_columns = ["employment_status", "exempt_status"]
    inspect_text_values(df, text_columns)

    # 3) Standardize
    df_clean = standardize_text_columns(df, MAPPINGS)

    # 4) Verify
    print("\nAfter standardization:")
    inspect_text_values(df_clean, text_columns)

    return df_clean
    


if __name__ == '__main__':
    initial_inspection(client_name="test_client",filename="jan_2024_test_data")