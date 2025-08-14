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

def compute_shift_length(df):
    """
    Computes total shift length in seconds for each row 
    where both clock_in and clock_out are present.

    Stores result in a new column called 'shift_length'.
    """
    # Initialize with NaN
    df["shift_length"] = pd.NA
    
    # Compute only valid pairs
    mask = df["clock_in"].notna() & df["clock_out"].notna()
    
    df.loc[mask, "shift_length"] = (
        df.loc[mask,"clock_out"] - df.loc[mask,"clock_in"]
    ).dt.total_seconds()
    
    return df 

def compute_time_to_1st_lunch(df):
    """
    Calculates the number of seconds between clock-in and the start of the first lunch break.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'clock_in' and 'lunch_start' datetime columns.

    Returns:
        pd.DataFrame: Updated DataFrame with a new column 'time_to_1st_lunch' in seconds.
    """
    df["time_to_1st_lunch"] = (df["lunch_start"] - df["clock_in"]).dt.total_seconds()
    return df

def compute_1st_lunch_duration(df):
    """
    Calculates the duration of the first lunch break in seconds.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'lunch_start' and 'lunch_end' datetime columns.

    Returns:
        pd.DataFrame: Updated DataFrame with a new column 'first_lunch_duration' in seconds.
    """
    df["first_lunch_duration"] = (df["lunch_end"] - df["lunch_start"]).dt.total_seconds()
    return df


def compute_time_to_2nd_lunch(df):
    """
    Calculates the number of seconds between clock-in and the start of the second lunch break.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'clock_in' and 'second_lunch_start' datetime columns.

    Returns:
        pd.DataFrame: Updated DataFrame with a new column 'time_to_2nd_lunch' in seconds.
    """
    df["time_to_2nd_lunch"] = (df["second_lunch_start"] - df["clock_in"]).dt.total_seconds()
    return df

def compute_2nd_lunch_duration(df):
    """
    Calculates the duration of the second lunch break in seconds.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'second_lunch_start' and 'second_lunch_end' datetime columns.

    Returns:
        pd.DataFrame: Updated DataFrame with a new column 'second_lunch_duration' in seconds.
    """
    df["second_lunch_duration"] = (df["second_lunch_end"] - df["second_lunch_start"]).dt.total_seconds()
    return df