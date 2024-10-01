import pandas as pd
import pyodbc

# Define connection parameters
server = 'your_server_name'  # e.g., 'localhost\SQLEXPRESS'
database = 'your_database_name'
username = 'your_username'
password = 'your_password'

# Establish a connection to the SQL Server database
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(connection_string)

# Load data from the source system (e.g., a CSV file or another database)
source_df = pd.read_excel('source_data.xlsx')

# Load data from the destination system (SQL Server)
query = "SELECT * FROM your_table_name"  # Replace with your actual table name
destination_df = pd.read_sql(query, conn)

# Step 1: Check row counts
source_row_count = len(source_df)
destination_row_count = len(destination_df)

if source_row_count == destination_row_count:
    print(f"Row count matches: {source_row_count} rows")
else:
    print(f"Row count mismatch! Source: {source_row_count}, Destination: {destination_row_count}")

# Step 2: Compare data field-by-field
mismatched_data = source_df.compare(destination_df, keep_equal=False)

if not mismatched_data.empty:
    print("Data mismatches found:")
    print(mismatched_data)
else:
    print("No data mismatches found.")

# Step 3: Check for duplicates in the destination system
duplicates_query = """
SELECT *, COUNT(*) 
FROM your_table_name 
GROUP BY your_columns 
HAVING COUNT(*) > 1
"""
duplicates_df = pd.read_sql(duplicates_query, conn)

if not duplicates_df.empty:
    print("Duplicates found in the destination data:")
    print(duplicates_df)
else:
    print("No duplicates found in the destination data.")

# Close the connection
conn.close()
