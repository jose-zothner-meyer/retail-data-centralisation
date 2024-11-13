from class_1_data_extractor import DataExtractor
from class_2_database_connector import DatabaseConnector
from class_3_data_cleaning import DataCleaning

def main():
    # Step 1: Initialize instances of the classes
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()

    # Step 2: Define table names
    source_table_name = 'legacy_users'  # Table containing user data
    target_table_name = 'dim_users'  # Target table to upload cleaned user data

    # Step 3: Extract, clean, and upload user data
    try:
        data_extractor.extract_and_upload_cleaned_user_data(
            db_connector=db_connector,
            cleaning_instance=data_cleaner,
            table_name=source_table_name,
            target_table_name=target_table_name
        )
    except ValueError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
