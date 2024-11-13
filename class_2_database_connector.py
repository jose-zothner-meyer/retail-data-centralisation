# Step 2: Database Utility Script
# File: database_utils.py

from typing import Dict
import pandas as pd
import yaml
from sqlalchemy import create_engine
import sqlalchemy

class DatabaseConnector:
    """
    Utility class to connect with and upload data to a database.
    """
    
    def connect_to_database(self, db_config: Dict[str, str]):
        """
        Establish a connection to the database.
        Args:
            db_config (Dict[str, str]): Configuration dictionary containing database credentials.
        Returns:
            Any: A connection object to the database.
        """
        pass
    
    def upload_to_database(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """
        Upload a pandas DataFrame to the database.
        Args:
            dataframe (pandas.DataFrame): The DataFrame containing the data to upload.
            table_name (str): The name of the table to upload the data to.
        """
        pass

    def read_db_creds(self, creds_file: str) -> Dict[str, str]:
        """
        Read the database credentials from a yaml file.
        Args:
            creds_file (str): Path to the yaml file containing database credentials.
        Returns:
            Dict[str, str]: A dictionary of database credentials.
        """
        with open(creds_file, 'r') as file:
            creds = yaml.safe_load(file)
        # Normalize credential keys to be lowercase for consistency
        normalized_creds = {key.lower(): value for key, value in creds.items()}
        return normalized_creds
    
    def init_db_engine(self, creds: Dict[str, str]):
        """
        Initialize and return an SQLAlchemy database engine using provided credentials.
        Args:
            creds (Dict[str, str]): Database credentials.
        Returns:
            sqlalchemy.engine.Engine: A SQLAlchemy engine instance.
        """
        engine = create_engine(f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}")
        return engine

    def list_db_tables(self, engine) -> list:
        """
        List all tables in the connected database.
        Args:
            engine: The SQLAlchemy engine connected to the database.
        Returns:
            List[str]: A list of table names in the database.
        """
        inspector = sqlalchemy.inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, dataframe: pd.DataFrame, table_name: str, engine) -> None:
        """
        Upload a pandas DataFrame to a database table using the provided engine.
        Args:
            dataframe (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the database table to upload data to.
            engine: The SQLAlchemy engine connected to the database.
        """
        dataframe.to_sql(table_name, con=engine, if_exists='replace', index=False)
