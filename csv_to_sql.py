import pandas as pd
import mysql.connector
from mysql.connector import Error
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sales.csv', 'sales'),
    ('products.csv', 'products'),
    ('delivery.csv', 'delivery'),
    ('payments.csv', 'payments')  # Added payments.csv for specific handling
]

# Folder containing the CSV files
folder_path = 'path_to_your_folder'

def get_sql_type(dtype):
    """Map pandas dtype to SQL data type."""
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

def create_table(cursor, table_name, df):
    """Create table query from DataFrame columns."""
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

def batch_insert(cursor, table_name, df):
    """Insert data in batches."""
    # Prepare the insert statement
    columns = ', '.join([f'`{col}`' for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
    
    # Convert DataFrame rows to list of tuples
    data = [tuple(None if pd.isna(x) else x for x in row) for _, row in df.iterrows()]
    
    # Batch insert (1000 rows at a time)
    batch_size = 1000
    for i in range(0, len(data), batch_size):
        cursor.executemany(insert_query, data[i:i+batch_size])

def main():
    """Main function to process CSV files."""
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host='your_host',
            user='your_username',
            password='your_password',
            database='your_database'
        )
        cursor = conn.cursor()

        # Process each CSV file
        for csv_file, table_name in csv_files:
            file_path = os.path.join(folder_path, csv_file)
            
            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(file_path)
            
            # Clean DataFrame
            df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
            df = df.where(pd.notnull(df), None)  # Handle NaN as None for SQL

            # Debugging: Print summary
            print(f"Processing {csv_file}:")
            print(f"NaN values replaced: {df.isnull().sum()}")
            
            # Create the table if it doesn't exist
            create_table(cursor, table_name, df)
            
            # Insert data in batches
            batch_insert(cursor, table_name, df)
            
            # Commit after processing each CSV file
            conn.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
