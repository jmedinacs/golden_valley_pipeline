'''
Created on Aug 12, 2025

@author: jarpy
'''

import pandas as pd 
from datetime import timedelta 

def convert_dates_and_datetimes(df, date_cols=None, datetime_cols=None):
    """
    Converts string columns to date or datetime format while preserving nulls.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
        date_cols (list): Columns to convert to date-only (normalized datetime64).
        datetime_cols (list): Columns to convert to full datetime.
    
    Returns:
        pd.DataFrame: Updated DataFrame with proper date/datetime types.
    """
    date_cols = date_cols or []
    datetime_cols = datetime_cols or []

    # Convert to date-only
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()

    # Convert to full datetime
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    return df

