# Step 1: Data Extraction Script
# File: data_extraction.py

from typing import Dict, Optional
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from class_2_database_connector import DatabaseConnector
from class_3_data_cleaning import DataCleaning

class DataExtractor:
    """
    Utility class to extract data from different data sources such as CSV files, APIs, and S3 buckets.
    """
    
    def extract_from_csv(self, file_path: str) -> pd.DataFrame:
        """
        Extract data from a CSV file.
        Args:
            file_path (str): The path to the CSV file.
        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        pass
    
    def extract_from_api(self, api_url: str, headers: Optional[Dict[str, str]] = None, params: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        Extract data from an API endpoint.
        Args:
            api_url (str): The URL of the API endpoint.
            headers (Optional[Dict[str, str]]): Optional headers for the API request.
            params (Optional[Dict[str, str]]): Optional parameters for the API request.
        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        pass

    def extract_from_s3(self, bucket_name: str, file_key: str, aws_credentials: Dict[str, str]) -> pd.DataFrame:
        """
        Extract data from an S3 bucket.
        Args:
            bucket_name (str): The name of the S3 bucket.
            file_key (str): The key of the file within the bucket.
            aws_credentials (Dict[str, str]): AWS credentials for accessing the bucket.
        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        pass

    def read_data_from_db(self, engine, table_name: str) -> pd.DataFrame:
        """
        Read data from a database table using the provided engine.
        Args:
            engine: The SQLAlchemy engine connected to the database.
            table_name (str): The name of the table to read the data from.
        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        query = f"SELECT * FROM {table_name}"
        dataframe = pd.read_sql(query, con=engine)
        return dataframe

    def read_rds_table(self, db_connector: 'DatabaseConnector', table_name: str) -> pd.DataFrame:
        """
        Extract the database table to a pandas DataFrame using the DatabaseConnector instance.
        Args:
            db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
            table_name (str): The name of the table to extract.
        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        engine = db_connector.init_db_engine(db_connector.read_db_creds('db_creds.yaml'))
        return self.read_data_from_db(engine, table_name)

    def retrieve_pdf_data(self, pdf_link: str) -> pd.DataFrame:
        """
        Extract data from a PDF document.
        Args:
            pdf_link (str): The link to the PDF document.
        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        # Extracting all pages from the PDF document
        dataframes = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        # Concatenate all the tables into a single DataFrame
        combined_dataframe = pd.concat(dataframes, ignore_index=True)
        return combined_dataframe

    def extract_and_upload_cleaned_user_data(self, db_connector: 'DatabaseConnector', cleaning_instance: 'DataCleaning', table_name: str, target_table_name: str) -> None:
        """
        Extract, clean, and upload user data to the target database table.
        Args:
            db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
            cleaning_instance (DataCleaning): An instance of the DataCleaning class.
            table_name (str): The name of the table to extract user data from.
            target_table_name (str): The name of the target table to upload cleaned data to.
        """
        # Step 1: Extract the data from the RDS table
        raw_data = self.read_rds_table(db_connector, table_name)
        print(f"Extracted raw data rows: {len(raw_data)}")  # Debug statement

        # Step 2: Clean the user data
        cleaned_data = cleaning_instance.clean_user_data(raw_data)

        # Step 3: Check the cleaned data row count and ensure correctness
        if len(cleaned_data) != 15284:
            print("\nDEBUG: After cleaning, unexpected row count encountered.")
            raise ValueError(f"Expected 15284 rows after cleaning, but got {len(cleaned_data)} rows.")

        # Step 4: Upload the cleaned data to the target table
        engine = db_connector.init_db_engine(db_connector.read_db_creds('db_creds.yaml'))
        db_connector.upload_to_db(cleaned_data, target_table_name, engine)
        print(f"Uploaded cleaned data to table: {target_table_name}")  # Debug statement
