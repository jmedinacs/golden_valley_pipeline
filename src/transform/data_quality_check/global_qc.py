'''
Created on Aug 12, 2025

@author: jarpy
'''

import util.time_helpers as th 
import util.main_data_io as mainIO
import pandas as pd 
import reporting.report_incomplete_rows as repInc

def date_and_time_conversions(df):
    """
    Converts specified date and datetime columns to appropriate datetime formats.

    This function is typically used after loading raw or cleaned timecard data to ensure:
    - 'date' and 'pay_date' are stored as normalized date-only columns
    - clock and lunch-related fields are properly parsed as datetime objects with time

    Assumes the existence of a helper function `convert_dates_and_datetimes(df, date_cols, datetime_cols)`
    in the `th` (transformation_helpers) module.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing timecard data.

    Returns:
        pd.DataFrame: DataFrame with date and time fields converted to proper datetime formats.
    """
    # Columns that should retain only date information (e.g., for grouping)
    date_cols = ["date", "pay_date"]

    # Columns that contain time information and must retain full datetime
    datetime_cols = ["clock_in", "clock_out", "lunch_start", "lunch_end"]

    # Convert using a shared helper function
    df_time_converted = th.convert_dates_and_datetimes(df, date_cols, datetime_cols)

    return df_time_converted

def flag_missing_critical_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'data_issues' column listing critical missing or inconsistent values for each row.

    This check is used during initial data quality review to identify rows that are:
    - Missing clock times
    - Have partial lunch entries
    - Missing key pay or employment information

    Parameters:
        df (pd.DataFrame): The cleaned DataFrame to inspect.

    Returns:
        pd.DataFrame: Same DataFrame with new 'data_issues' column listing per-row problems.
    """
    issues = []

    for _, row in df.iterrows():
        row_issues = []

        # Required time columns
        if pd.isna(row.get("clock_in")):
            row_issues.append("missing clock_in")
        if pd.isna(row.get("clock_out")):
            row_issues.append("missing clock_out")

        # Partial lunch issues
        if pd.notna(row.get("lunch_start")) and pd.isna(row.get("lunch_end")):
            row_issues.append("missing lunch_end (partial lunch)")
        if pd.notna(row.get("lunch_end")) and pd.isna(row.get("lunch_start")):
            row_issues.append("missing lunch_start (partial lunch)")
        if pd.notna(row.get("second_lunch_start")) and pd.isna(row.get("second_lunch_end")):
            row_issues.append("missing second_lunch_end (partial second lunch)")
        if pd.notna(row.get("second_lunch_end")) and pd.isna(row.get("second_lunch_start")):
            row_issues.append("missing second_lunch_start (partial second lunch)")

        # Required pay fields
        if pd.isna(row.get("wage_rate")):
            row_issues.append("missing wage_rate")
        if pd.isna(row.get("total_pay_actual")):
            row_issues.append("missing total_pay_actual")
        if pd.isna(row.get("pay_date")):
            row_issues.append("missing pay_date")

        # Required employee info
        if pd.isna(row.get("employment_status")):
            row_issues.append("missing employment_status")
        if pd.isna(row.get("exempt_status")):
            row_issues.append("missing exempt_status")
        if pd.isna(row.get("employee_id")):
            row_issues.append("missing employee_id")
        if pd.isna(row.get("date")):
            row_issues.append("missing work date")

        issues.append(row_issues if row_issues else None)

    df["data_issues"] = issues
    return df

def fill_missing_waivers_with_false(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing waiver values with False, based on the assumption that blanks
    mean no waiver was signed.

    Parameters:
        df (pd.DataFrame): The DataFrame with potential nulls in waiver fields.

    Returns:
        pd.DataFrame: Updated DataFrame with missing waivers defaulted to False.
    """
    df["first_meal_waiver_signed"] = df["first_meal_waiver_signed"].fillna(False)
    df["second_meal_waiver_signed"] = df["second_meal_waiver_signed"].fillna(False)
    return df

def check_dq_all(client_name: str = "missing_client", filename: str = "missing_filename", corrected: bool = False):
    """
    Runs the full data quality check pipeline on cleaned client data.

    Steps:
    - Load cleaned dataset
    - Normalize date and datetime columns
    - Fill missing meal waiver values with False
    - Identify rows with missing critical fields
    - Export report of incomplete rows for client review

    Returns:
        pd.DataFrame: Full dataset with a new 'data_issues' column
                      (incomplete rows are not dropped).
    """
    if corrected:
        df = mainIO.load_corrected_data(client_name, filename)
    else: 
        df = mainIO.load_cleaned_data(client_name, filename)

    df = date_and_time_conversions(df)
    
    df = fill_missing_waivers_with_false(df)
    
    df_evaluated = flag_missing_critical_fields(df)
    
    df_missing = df_evaluated[df_evaluated["data_issues"].notna()]
    
    #repInc.report_incomplete_rows_global(df_missing, client_name, filename)
    repInc.report_incomplete_rows_global(df_missing, client_name, "testing_test")
    
    print("Number of incomplete rows reported ",df_missing.shape[0])
    
    return df_evaluated 

# FIX ME: handle merging logic, so if corrected - merge data first and save, then load and run eval

   

if __name__ == '__main__':
    check_dq_all(client_name = "test_client", filename = "jan_2024_test_data",corrected = False)