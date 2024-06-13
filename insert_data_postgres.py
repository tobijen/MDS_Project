import os
import os
import pandas as pd
import psycopg2
from psycopg2 import sql

# Define the directory containing CSV files
directory = 'csv_data'  # Update with the actual path to your folder

# PostgreSQL connection parameters (update with your database details)
db_params = {
    'dbname': 'postgres',    # Replace with your database name
    'user': 'postgres',     # Replace with your database username
    'password': 'pass', # Replace with your database password
    'host': 'localhost',         # Replace with your database host
    'port': '5432'               # Replace with your database port (default is 5432)
}

# Mapping from Pandas data types to PostgreSQL data types
type_mapping = {
    'int64': 'INTEGER',
    'object': 'TEXT',
    'float64': 'FLOAT'
}

# Connect to the PostgreSQL database
conn = None
try:
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Iterate over each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)

            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)

            # Create the dictionary to hold file information
            file_info = {
                'file_name': filename,
                'columns': []
            }

            # Iterate over each column in the DataFrame
            for column in df.columns:
                column_info = {
                    column: str(df[column].dtype)
                }
                file_info['columns'].append(column_info)

            # Generate SQL CREATE TABLE statement
            table_name = file_info['file_name'].replace('.csv', '')
            columns = file_info['columns']

            # Constructing column definitions
            column_definitions = []
            for column in columns:
                column_name = list(column.keys())[0]
                column_type = list(column.values())[0]
                postgres_type = type_mapping.get(column_type, 'TEXT')  # Default to TEXT if type is not found
                column_definitions.append(f"{column_name} {postgres_type}")

            # Join column definitions with commas
            columns_sql = ', '.join(column_definitions)
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"

            # Print the SQL statement for reference (optional)
            print(f"Generated SQL for {filename}: {create_table_sql}")

            # Execute the SQL statement to create the table
            cur.execute(create_table_sql)
            conn.commit()
            print(f"Table '{table_name}' created successfully for {filename}.")

            # Inserting the data into the PostgreSQL table
            # Convert dataframe to list of tuples
            tuples = [tuple(x) for x in df.to_numpy()]
            cols = ','.join(list(df.columns))

            # Generate INSERT INTO statement
            insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(table_name),
                sql.SQL(cols)
            )

            # Use psycopg2's execute_values to insert the data in bulk
            from psycopg2.extras import execute_values
            execute_values(cur, insert_sql, tuples)
            conn.commit()
            print(f"Data from '{filename}' inserted successfully into '{table_name}'.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if conn:
        cur.close()
        conn.close()