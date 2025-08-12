'''
Created on Aug 9, 2025

@author: jarpy
'''

import util.main_data_io as mainIO


def inspect_data(df):
    """ """
    print(df.info())
    print("\nNumber of null entries per column\n")
    print(df.isnull().sum())
    
def inspect_text_values(df, text_cols):
    """ """
    for col in text_cols:
        print(df[col].value_counts(dropna=False))
    


def initial_inspection(client_name="missing_client",filename="missing_filename"):
    """ """
    # Load the raw data from the client's data/raw folder
    df = mainIO.load_raw_data(client_name, filename)
    # Initial inspection of data for data type and number of null
    inspect_data(df)
    
    text_columns = ["employment_status","exempt_status"]
    inspect_text_values(df, text_columns)
    


if __name__ == '__main__':
    initial_inspection(client_name="test_client",filename="jan_2024_test_data")