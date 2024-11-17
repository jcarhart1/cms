import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Database connection parameters
db_params = {
    'dbname': 'cms',
    'user': 'postgres',
    'password': 'password',  # Replace with your actual RDS password
    'host': 'postgres-db-dev.c7sosyk2g2tx.us-east-1.rds.amazonaws.com',
    'port': '5432'
}

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Base directory containing the data folders
base_dir = '/Users/jcarhart/Desktop/code-cane/cms/data'


# Function to map pandas dtypes to PostgreSQL types
def get_postgres_type(pandas_type):
    if pd.api.types.is_integer_dtype(pandas_type):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(pandas_type):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(pandas_type):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(pandas_type):
        return 'TIMESTAMP'
    else:
        return 'TEXT'


# Function to create table and import data
def process_csv(file_path, table_name):
    # Read CSV file
    df = pd.read_csv(file_path, nrows=0)  # Read only the header

    # Get column names and types
    columns = df.columns.tolist()
    types = df.dtypes.tolist()

    # Create table
    columns_with_types = [f'"{col}" {get_postgres_type(typ)}' for col, typ in zip(columns, types)]
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_with_types)})"
    cur.execute(create_table_query)

    # Import data
    with open(file_path, 'r') as f:
        next(f)  # Skip the header
        quoted_columns = [f'"{col}"' for col in columns]
        copy_sql = f"COPY {table_name} ({','.join(quoted_columns)}) FROM STDIN CSV"
        cur.copy_expert(sql=copy_sql, file=f)

    conn.commit()
    print(f"Imported {file_path} to table {table_name}")


# Process each subdirectory
for subdir in ['bene', 'ip', 'op']:
    dir_path = os.path.join(base_dir, subdir)
    for filename in os.listdir(dir_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(dir_path, filename)
            table_name = f"{subdir}_{os.path.splitext(filename)[0]}"
            process_csv(file_path, table_name)

# Close database connection
cur.close()
conn.close()

print("All data imported successfully!")