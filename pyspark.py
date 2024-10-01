#connect pyspark to a sql server

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SQLServerDataValidation") \
    .config("spark.jars", "/path/to/mssql-jdbc-9.4.1.jre8.jar") \
    .getOrCreate()

# SQL Server connection properties
jdbc_url = "jdbc:sqlserver://your_server_name:1433;databaseName=your_database_name"
db_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Load data from the SQL Server table
destination_df = spark.read.jdbc(url=jdbc_url, table="your_table_name", properties=db_properties)

# Load data from the source system (e.g., a CSV file or another source)
source_df = spark.read.csv("source_data.csv", header=True, inferSchema=True)

# Step 1: Check row counts
source_row_count = source_df.count()
destination_row_count = destination_df.count()

if source_row_count == destination_row_count:
    print(f"Row count matches: {source_row_count} rows")
else:
    print(f"Row count mismatch! Source: {source_row_count}, Destination: {destination_row_count}")

# Step 2: Compare data field-by-field using left anti join to find mismatched rows
mismatched_rows = source_df.join(destination_df, on=[source_df[c] == destination_df[c] for c in source_df.columns], how='leftanti')

if mismatched_rows.count() > 0:
    print("Data mismatches found:")
    mismatched_rows.show()
else:
    print("No data mismatches found.")

# Step 3: Check for duplicates in the destination system
destination_df.createOrReplaceTempView("destination_table")
duplicates = spark.sql("""
    SELECT *, COUNT(*) as count 
    FROM destination_table 
    GROUP BY column1, column2  -- Replace with actual column names
    HAVING count > 1
""")

if duplicates.count() > 0:
    print("Duplicates found in the destination data:")
    duplicates.show()
else:
    print("No duplicates found in the destination data.")

# Stop the Spark session
spark.stop()
