# Step 3: Data Cleaning Script
# File: data_cleaning.py

import pandas as pd
import re

class DataCleaning:
    """
    Utility class to clean data from various data sources.
    """
    
    def clean_csv_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data extracted from a CSV file.
        Args:
            dataframe (pandas.DataFrame): The DataFrame to be cleaned.
        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        pass
    
    def clean_api_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data extracted from an API source.
        Args:
            dataframe (pandas.DataFrame): The DataFrame to be cleaned.
        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        pass
    
    def clean_card_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Clean card data extracted from the PDF.
        Args:
            dataframe (pandas.DataFrame): The DataFrame containing card data to be cleaned.
        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        # Drop any completely empty columns
        dataframe.dropna(axis=1, how='all', inplace=True)
        # Drop rows where all columns are NaN
        dataframe.dropna(axis=0, how='all', inplace=True)
        # Remove rows with invalid card numbers (e.g., non-numeric or too short/long values)
        if 'card_number' in dataframe.columns:
            dataframe = dataframe[dataframe['card_number'].str.isnumeric()]
            dataframe = dataframe[dataframe['card_number'].str.len() == 16]
        # Convert necessary columns to appropriate dtypes (e.g., dates, numeric values)
        if 'expiry_date' in dataframe.columns:
            dataframe['expiry_date'] = pd.to_datetime(dataframe['expiry_date'], errors='coerce', format='%m/%y')
        if 'security_code' in dataframe.columns:
            dataframe['security_code'] = pd.to_numeric(dataframe['security_code'], errors='coerce')
        # Drop any rows with remaining NaN values after type conversion
        dataframe.dropna(inplace=True)
        # Remove duplicate rows if any
        dataframe.drop_duplicates(inplace=True)
        return dataframe

    def clean_user_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Clean user data extracted from the database.
        Args:
            dataframe (pandas.DataFrame): The DataFrame containing user data to be cleaned.
        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        # Change "NULL" strings into actual NaN values
        dataframe.replace("NULL", pd.NA, inplace=True)
        print(f"Rows with 'NULL' values replaced: {dataframe.isna().sum().sum()}")  # Debug statement

        # Selectively handle missing values
        # Fill missing values in important columns with defaults if appropriate
        columns_to_fill = ['first_name', 'last_name', 'email_address']
        for col in columns_to_fill:
            if col in dataframe.columns:
                dataframe.loc[:, col] = dataframe[col].fillna('Unknown')
        print(f"Rows after filling important columns: {len(dataframe)}")  # Debug statement

        # Convert join_date and date_of_birth columns to datetime if they exist
        if 'join_date' in dataframe.columns:
            dataframe.loc[:, 'join_date'] = pd.to_datetime(dataframe['join_date'], errors='coerce')
        if 'date_of_birth' in dataframe.columns:
            dataframe.loc[:, 'date_of_birth'] = pd.to_datetime(dataframe['date_of_birth'], errors='coerce')

        # Fill invalid dates with a default value rather than dropping rows
        if 'join_date' in dataframe.columns:
            dataframe.loc[:, 'join_date'] = dataframe['join_date'].fillna(pd.Timestamp('1900-01-01'))
        if 'date_of_birth' in dataframe.columns:
            dataframe.loc[:, 'date_of_birth'] = dataframe['date_of_birth'].fillna(pd.Timestamp('1900-01-01'))

        # Remove rows with remaining NULL values in critical columns
        critical_columns = ['user_uuid', 'join_date']
        dataframe_before_dropping_nulls = len(dataframe)
        null_critical_values = dataframe[dataframe[critical_columns].isna().any(axis=1)]
        print(f"Rows with missing critical values before dropping: {len(null_critical_values)}")
        if not null_critical_values.empty:
            print("Rows with missing critical values:\n", null_critical_values)
        dataframe = dataframe.dropna(subset=critical_columns)
        rows_dropped_due_to_nulls = dataframe_before_dropping_nulls - len(dataframe)
        print(f"Rows dropped due to missing critical values: {rows_dropped_due_to_nulls}")  # Debug statement

        # Normalize data for better duplicate detection
        for col in ['first_name', 'last_name', 'email_address', 'company', 'address', 'country']:
            if col in dataframe.columns:
                dataframe.loc[:, col] = dataframe[col].str.lower().str.strip()
                # Remove punctuation and special characters
                dataframe.loc[:, col] = dataframe[col].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)) if pd.notna(x) else x)

        # Fill all remaining NaN values to avoid hidden inconsistencies
        dataframe.fillna('Unknown', inplace=True)

        # Drop duplicate rows based on key columns only
        key_columns = ['user_uuid']
        duplicates_key = dataframe.duplicated(subset=key_columns)
        duplicate_rows_key = dataframe[duplicates_key]
        print(f"Duplicate rows based on key columns to be removed: {len(duplicate_rows_key)}")
        if not duplicate_rows_key.empty:
            print("Duplicate rows based on key columns:\n", duplicate_rows_key)
        dataframe = dataframe[~duplicates_key]

        # Check for and drop duplicate rows across all columns if any
        duplicates_all = dataframe[dataframe.duplicated()]
        print(f"Duplicate rows across all columns to be removed: {len(duplicates_all)}")  # Debug statement
        if not duplicates_all.empty:
            print("Duplicate rows across all columns:\n", duplicates_all)
        dataframe = dataframe.drop_duplicates()

        # Deeper analysis of possible duplicates based on multiple columns
        grouped = dataframe.groupby(['first_name', 'last_name', 'date_of_birth'])
        possible_duplicates = grouped.filter(lambda x: len(x) > 1)
        print(f"Possible non-exact duplicates found: {len(possible_duplicates)}")
        if not possible_duplicates.empty:
            print(possible_duplicates)

        # Drop non-exact duplicates but keep the first occurrence
        dataframe = dataframe.drop_duplicates(subset=['first_name', 'last_name', 'date_of_birth'], keep='first')
        print(f"After dropping non-exact duplicates, final rows: {len(dataframe)}")

        # Manually inspect the last 15 rows to see if they are unexpected
        extra_rows = dataframe.tail(15)
        print("\nInspecting last 15 rows:\n", extra_rows)

        # Final debug statement
        print(f"Final cleaned data rows: {len(dataframe)}")  # Debug statement

        return dataframe
