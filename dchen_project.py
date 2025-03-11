#!/usr/bin/env python
# coding: utf-8

# # Student: Darren Chen
# # ADS 507 
# # Assignment 5.1  - SQL Query Performance Improvement

# In[1]:


# kernel python 3.10.12 
# Install the mysql-connector-python package
# %pip install mysql
# %pip install mysql-connector-python

import mysql.connector 
import pandas as pd
import csv


# Connect to Azure Database

# In[1]:


# function to connect to the database
def connect_to_db(database_name):
    # Connect to the MySQL database
    db_config = {
        "host": "mysqldchen.mysql.database.azure.com",
        "user": "dchenAdmin",
        "password": "507password!",
        "database": database_name,   
        "port": "3306"
    }
    return mysql.connector.connect(**db_config)


# In[ ]:


# run_query function to execute SQL queries
def run_query(query):
    # try: (indent next for error handling)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)


# In[ ]:


def create_table(cursor, table_name, columns, data_types, primary_keys):
    """ DOCSTRING:
    Creates a table in the database with the specified columns and data types.

    Args:
        cursor (Cursor): Database cursor to execute SQL commands.
        table_name (str): Name of the table to be created.
        columns (list of str): List of column names for the table.
        data_types (list of str): Corresponding data types for each column.
        primary_keys (list of str): List of columns to set as primary keys.

    Returns:
        None

    Raises:
        ValueError: If the lengths of `columns` and `data_types` do not match.
    """
    dtype_mapping = {
        "int64": "INT",
        "float64": "FLOAT",
        "object": "TEXT",
        "bool": "BOOLEAN"
    }
    
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for col, dtype in zip(columns, data_types):
        mysql_type = dtype_mapping.get(str(dtype), "TEXT")
        create_table_query += f"`{col}` {mysql_type}, "
    
    create_table_query += f"PRIMARY KEY ({', '.join(primary_keys)}));"
    cursor.execute(create_table_query)


# Load ADSS data to sql datab

# ETL DATA: Alcohol and Drug Services Study (ADSS), 1996-1999: [United States] (ICPSR 3088)
# LINK: https://www.icpsr.umich.edu/web/NAHDAP/studies/3088/export
# DOWNLOAD DELIMITED FILE
# 

# In[ ]:


# convert TSV to CSV

# Define the file paths 
tsv_file_path = r'C:\Users\darre\OneDrive\Documents\ADS\ADS 507 Data Engineering\data\ICPSR_03088\DS0003\03088-0003-Data.tsv'
csv_file_path = r'C:\Users\darre\OneDrive\Documents\ADS\ADS 507 Data Engineering\data\ICPSR_03088\DS0003\03088-0003-Data.csv'

# Open the TSV file for reading and the CSV file for writing
with open(tsv_file_path, 'r', newline='', encoding='utf-8') as tsv_file, \
     open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
    
    # Create a TSV reader and a CSV writer
    tsv_reader = csv.reader(tsv_file, delimiter='\t')
    csv_writer = csv.writer(csv_file, delimiter=',')

    # Iterate over each row in the TSV file and write it to the CSV file
    for row in tsv_reader:
        csv_writer.writerow(row)

print(f"TSV file has been successfully converted to CSV and saved at: {csv_file_path}")


# convert tsv file to csv for import to mysql database: 
# 
# in VSCode Terminal enter the following to convert to csv:  
# >Import-Csv -Delimiter "`t" -Path "C:\Users\darre\OneDrive\Documents\ADS\ADS 507 Data Engineering\data\ICPSR_03088\DS0004\03088-0004-Data.tsv" | Export-Csv -NoTypeInformation -Path "C:\Users\darre\OneDrive\Documents\ADS\ADS 507 Data Engineering\data\ICPSR_03088\DS0004\03088-0004-Data.csv"

# ICPSR_03088  Alcohol and Drug Services Study (ADSS)
# 
# Part 3 - Phase 2 - Main Incentive Abstract data table
# 

# In[9]:


# Define file path
csv_file = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0003\\03088-0003-Data.csv"

# Load CSV into DataFrame
df_part3 = pd.read_csv(csv_file)

# Check if 'group' column exists before dropping
if 'group' in df_part3.columns:
    df_part3 = df_part3.drop(columns=['group'])
    print("Column 'group' has been removed.")
else:
    print("Column 'group' not found in the dataset.")

# Save updated DataFrame to a new CSV file
output_file = csv_file.replace(".csv", "_updated.csv")
df_part3.to_csv(output_file, index=False)

print(f"Updated file saved as: {output_file}")


# In[10]:


# import mysql.connector
# import pandas as pd

# Database connection details
db_config = {
    "host": "mysqldchen.mysql.database.azure.com",
    "user": "dchenAdmin",
    "port": 3306,
    "password": "507password!",
    "database": "icpsr_03088"
}

# Connect to MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Generate table name
table_name = "adss_data_part3"

# Define file path
csv_file = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0003\\03088-0003-Data_updated.csv"

# Load CSV into DataFrame
df_part3 = pd.read_csv(csv_file)

# Get column names and data types
columns = df_part3.columns
data_types = df_part3.dtypes

# Mapping pandas dtypes to MySQL column types
dtype_mapping = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "TEXT",
    "bool": "BOOLEAN"
}

# Define the table schema with caseid and facid as PRIMARY KEYS
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
for col, dtype in zip(columns, data_types):
    mysql_type = dtype_mapping.get(str(dtype), "TEXT")  # Default to TEXT if unknown
    create_table_query += f"`{col}` {mysql_type}, "

# Define composite primary key (caseid, facid)
create_table_query += "PRIMARY KEY (`caseid`, `facid`));"

# Execute the table creation query
cursor.execute(create_table_query)
conn.commit()

# Insert data into the table
insert_query = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
# iteratively adds rows to the table
for _, row in df_part3.iterrows():
    cursor.execute(insert_query, tuple(row))

# Commit and close the connection
conn.commit()
cursor.close()
conn.close()

print("Data successfully loaded into MySQL database.")


# ICPSR_03088  Alcohol and Drug Services Study (ADSS)
# 
# Part 4 - Phase 2 - In-Treatment Methadone Abstract
# 

# In[14]:


# Define file path
csv_file = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0004\\03088-0004-Data.csv"

# Load CSV into DataFrame
df_part4 = pd.read_csv(csv_file)

# Check if 'group' column exists before dropping
if 'group' in df_part4.columns:
    df_part4 = df_part4.drop(columns=['group'])
    print("Column 'group' has been removed.")
else:
    print("Column 'group' not found in the dataset.")

# Save updated DataFrame to a new CSV file
output_file = csv_file.replace(".csv", "_updated.csv")
df_part4.to_csv(output_file, index=False)

print(f"Updated file saved as: {output_file}")


# In[ ]:





# In[15]:


# Database connection details
db_config = {
    "host": "mysqldchen.mysql.database.azure.com",
    "user": "dchenAdmin",
    "port": 3306,
    "password": "507password!",
    "database": "icpsr_03088"
}

# Connect to MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Generate table name
table_name = "adss_data_part4"

# Define file path
csv_file = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0004\\03088-0004-Data_updated.csv"

# Load CSV into DataFrame
df_part4 = pd.read_csv(csv_file)

# Get column names and data types
columns = df_part4.columns
data_types = df_part4.dtypes

# Mapping pandas dtypes to MySQL column types
dtype_mapping = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "TEXT",
    "bool": "BOOLEAN"
}

# Define the table schema with caseid and facid as PRIMARY KEYS
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
for col, dtype in zip(columns, data_types):
    mysql_type = dtype_mapping.get(str(dtype), "TEXT")  # Default to TEXT if unknown
    create_table_query += f"`{col}` {mysql_type}, "

# Define composite primary key (caseid, facid)
create_table_query += "PRIMARY KEY (`caseid`, `facid`));"

# Execute the table creation query
cursor.execute(create_table_query)
conn.commit()

# Insert data into the table
insert_query = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
# iteratively adds rows to the table
for _, row in df_part4.iterrows():
    cursor.execute(insert_query, tuple(row))

# Commit and close the connection
conn.commit()
cursor.close()
conn.close()

print("Data successfully loaded into MySQL database.")


# ICPSR_03088  Alcohol and Drug Services Study (ADSS)
# 
# Part 7 - Phase 3 - Follow-up Survey

# In[16]:


# Load the data from tsv and remove column named 'group'
# Define file path
csv_file = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0007\\03088-0007-Data.csv"

# Load CSV into DataFrame
df_part7 = pd.read_csv(csv_file)

# Check if 'group' column exists before dropping
if 'group' in df_part7.columns:
    df_part7 = df_part7.drop(columns=['group'])
    print("Column 'group' has been removed.")
else:
    print("Column 'group' not found in the dataset.")

# Save updated DataFrame to a new CSV file
output_file = csv_file.replace(".csv", "_updated.csv")
df_part7.to_csv(output_file, index=False)

print(f"Updated file saved as: {output_file}")


# In[17]:


# Database connection details
db_config = {
    "host": "mysqldchen.mysql.database.azure.com",
    "user": "dchenAdmin",
    "port": 3306,
    "password": "507password!",
    "database": "icpsr_03088"
}

# Connect to MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Generate table name
table_name = "adss_data_part7"

# Define file path
csv_file = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0007\\03088-0007-Data_updated.csv"

# Load CSV into DataFrame
df_part7 = pd.read_csv(csv_file)

# Get column names and data types
columns = df_part7.columns
data_types = df_part7.dtypes

# Mapping pandas dtypes to MySQL column types
dtype_mapping = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "TEXT",
    "bool": "BOOLEAN"
}

# Define the table schema with caseid and facid as PRIMARY KEYS
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
for col, dtype in zip(columns, data_types):
    mysql_type = dtype_mapping.get(str(dtype), "TEXT")  # Default to TEXT if unknown
    create_table_query += f"`{col}` {mysql_type}, "

# Define composite primary key (caseid, facid)
create_table_query += "PRIMARY KEY (`caseid`, `facid`));"

# Execute the table creation query
cursor.execute(create_table_query)
conn.commit()

# Insert data into the table
insert_query = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
# iteratively adds rows to the table
for _, row in df_part7.iterrows():
    cursor.execute(insert_query, tuple(row))

# Commit and close the connection
conn.commit()
cursor.close()
conn.close()

print("Data successfully loaded into MySQL database.")


# database: cdcyrbss   
# CDC - Youth Risk Behavior Surveillance System (YRBSS) 
# 
# creating schema, and importing data 
# 
# primary key = 'CASEID'

# In[ ]:


# 3ector
# import pandas as pd 

# Database connection details 
db_config = {
    "host": "mysqldchen.mysql.database.azure.com",
    "user": "dchenAdmin",
    "port": 3306,
    "password": "507password!",
    "database": "cdcyrbss"
}

# Connect to MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Load CSV file
csv_file = r"C:\Users\darre\OneDrive\Documents\ADS\ADS 507 Data Engineering\data\CDC\XXHq.csv"

# Read CSV into pandas DataFrame
cdcyrbss_df = pd.read_csv(csv_file)

# Convert column names to lowercase and remove spaces for compatibility
cdcyrbss_df.columns = cdcyrbss_df.columns.str.lower().str.replace(" ", "_")

# Table name
table_name = "national2021"

# Get column names and data types
columns = cdcyrbss_df.columns
data_types = cdcyrbss_df.dtypes

# Mapping pandas dtypes to MySQL column types
dtype_mapping = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "TEXT",
    "bool": "BOOLEAN"
}

# Define the table schema dynamically
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
column_definitions = []

for col, dtype in zip(columns, data_types):
    mysql_type = dtype_mapping.get(str(dtype), "TEXT")  # Default to TEXT if unknown
    column_definitions.append(f"`{col}` {mysql_type}")

# Add primary key (caseid)
column_definitions.append("PRIMARY KEY (`record`)")
create_table_query += ", ".join(column_definitions) + ");"

# Execute the table creation query
cursor.execute(create_table_query)
conn.commit()

# Insert data into the table
insert_query = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

for _, row in cdcyrbss_df.iterrows():
    cursor.execute(insert_query, tuple(row))

# Commit and close the connection
conn.commit()
cursor.close()
conn.close()

print(" Data successfully loaded into MySQL database.")


# In[ ]:


# Define file path
csv_file = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0004\\03088-0004-Data.csv"

# Load CSV into DataFrame
df_part4 = pd.read_csv(csv_file)

# Check if 'group' column exists before dropping
if 'group' in df_part4.columns:
    df_part4 = df_part4.drop(columns=['group'])
    print("Column 'group' has been removed.")
else:
    print("Column 'group' not found in the dataset.")

# Save updated DataFrame to a new CSV file
output_file = csv_file.replace(".csv", "_updated.csv")
df_part4.to_csv(output_file, index=False)

print(f"Updated file saved as: {output_file}")


# In[ ]:


# Database connection details
db_config = {
    "host": "mysqldchen.mysql.database.azure.com",
    "user": "dchenAdmin",
    "port": 3306,
    "password": "507password!",
    "database": "icpsr_03088"
}

# Connect to MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Generate table name
table_name = "adss_data_part4"

# Define file path
csv_file = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0004\\03088-0004-Data_updated.csv"

# Load CSV into DataFrame
df_part4 = pd.read_csv(csv_file)

# Get column names and data types
columns = df_part4.columns
data_types = df_part4.dtypes

# Mapping pandas dtypes to MySQL column types
dtype_mapping = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "TEXT",
    "bool": "BOOLEAN"
}

# Define the table schema with caseid and facid as PRIMARY KEYS
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
for col, dtype in zip(columns, data_types):
    mysql_type = dtype_mapping.get(str(dtype), "TEXT")  # Default to TEXT if unknown
    create_table_query += f"`{col}` {mysql_type}, "

# Define composite primary key (caseid, facid)
create_table_query += "PRIMARY KEY (`caseid`, `facid`));"

# Execute the table creation query
cursor.execute(create_table_query)
conn.commit()

# Insert data into the table
insert_query = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
# iteratively adds rows to the table
for _, row in df_part4.iterrows():
    cursor.execute(insert_query, tuple(row))

# Commit and close the connection
conn.commit()
cursor.close()
conn.close()

print("Data successfully loaded into MySQL database.")


# 

# database: dawn   
# Drug Abuse Warning Network (DAWN) 
# 
# creating schema, and importing data 
# 
# table name = 'data'
# 
# primary key = 'CASEID'

# In[7]:


# convert DAWN data TSV to CSV

# Define the file paths 
tsv_file_path = r'C:\Users\darre\OneDrive\Documents\ADS\ADS 507 Data Engineering\data\Drug Abuse Warning Network (DAWN) ICPSR_34565\DS0001\34565-0001-Data.tsv'
csv_file_path = r'C:\Users\darre\OneDrive\Documents\ADS\ADS 507 Data Engineering\data\Drug Abuse Warning Network (DAWN) ICPSR_34565\DS0001\34565-0001-Data.csv'

# Open the TSV file for reading and the CSV file for writing
with open(tsv_file_path, 'r', newline='', encoding='utf-8') as tsv_file, \
     open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
    
    # Create a TSV reader and a CSV writer
    tsv_reader = csv.reader(tsv_file, delimiter='\t')
    csv_writer = csv.writer(csv_file, delimiter=',')

    # Iterate over each row in the TSV file and write it to the CSV file
    for row in tsv_reader:
        csv_writer.writerow(row)

print(f"TSV file has been successfully converted to CSV and saved at: {csv_file_path}")


# import the table of the drug codes and drug labels into DAWN database

# In[10]:


# Define file path
file_path = r"C:/Users/darre/OneDrive/Documents/ADS/ADS 507 Data Engineering/data/Drug Abuse Warning Network (DAWN) ICPSR_34565/drug_lookup.csv"

# Read CSV into DataFrame
drug_lookup_df = pd.read_csv(file_path)

# Database connection details
db_config = {
    "host": "mysqldchen.mysql.database.azure.com",
    "user": "dchenAdmin",
    "port": 3306,
    "password": "507password!",
    "database": "dawn"
}

# Connect to MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Create the table in the DAWN database if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS druglookup (
    Value INT PRIMARY KEY,
    Drug_Label VARCHAR(255) NOT NULL
);
"""
cursor.execute(create_table_query)
conn.commit()

# Construct SQL INSERT statements using parameterized queries
insert_query = """
INSERT INTO druglookup (Value, Drug_Label)
VALUES (%s, %s)
"""

# Execute batch insert
for row in drug_lookup_df.itertuples(index=False):
    cursor.execute(insert_query, (row.Value, row.Drug_Label))

conn.commit()

# Close the connection
cursor.close()
conn.close()

print("CSV file successfully imported into dawn.druglookup.")


# Drug Abuse Warning Network (DAWN) Data load 
# 
# Due to large dataset size 229,212 records with 285 columns, use BULK INSERT.  
# 
# 1. Azure Created Storage Account (resource) and uploaded CSV file. 
# 2. 
# 
# 
# Option 1: Use BULK INSERT (Recommended for Large Data)
# 
#     Enable Bulk Insert:
# 
# EXEC sp_configure 'show advanced options', 1;
# RECONFIGURE;
# EXEC sp_configure 'Ad Hoc Distributed Queries', 1;
# RECONFIGURE;
# 
# Load the CSV from Azure Blob Storage:
# 
# BULK INSERT er_data
# FROM 'https://ads507storage.blob.core.windows.net/container1/34565-0001-Data.csv'
# WITH (
#     DATA_SOURCE = 'MyAzureBlobStorage',
#     FORMAT = 'CSV',
#     FIRSTROW = 2,  -- Skip header
#     FIELDTERMINATOR = ',',
#     ROWTERMINATOR = '\n',
#     TABLOCK
# );
# 
# 
# Create an External Data Source (One-Time Setup):
# 
# CREATE DATABASE SCOPED CREDENTIAL MyAzureCredential
# WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
# SECRET = 'your_sas_token';
# 
# CREATE EXTERNAL DATA SOURCE MyAzureBlobStorage
# WITH (
#     TYPE = BLOB_STORAGE,
#     LOCATION = 'https://ads507storage.blob.core.windows.net/csv-uploads',
#     CREDENTIAL = MyAzureCredential
# );
# 
# Now, BULK INSERT will work with your Blob Storage!

# In[6]:


# Database connection details
db_config = {
    "host": "mysqldchen.mysql.database.azure.com",
    "user": "dchenAdmin",
    "port": 3306,
    "password": "507password!",
    "database": "dawn"
}

# Connect to MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Load CSV file
csv_file = r"C:\Users\darre\OneDrive\Documents\ADS\ADS 507 Data Engineering\data\Drug Abuse Warning Network (DAWN) ICPSR_34565\DS0001\34565-0001-Data.csv"

# Read only first 5000 rows from CSV
dawn_df = pd.read_csv(csv_file, nrows=5000)

# Adjust column names
dawn_df.columns = dawn_df.columns.str.lower().str.replace(" ", "_")

table_name = "er_data"

columns = dawn_df.columns
data_types = dawn_df.dtypes

# Mapping pandas dtypes to MySQL
dtype_mapping = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "TEXT",
    "bool": "BOOLEAN"
}

# Define the table schema dynamically
column_definitions = ",\n".join([
    f"`{col}` {dtype_mapping.get(str(dtype), 'TEXT')} {'PRIMARY KEY' if col == 'caseid' else ''}".strip()
    for col, dtype in zip(columns, data_types)
])

# ✅ FIX: Define the missing `CREATE TABLE` query
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {column_definitions}
);
"""

# Execute table creation
cursor.execute(create_table_query)
conn.commit()
print("✅ Table created successfully.")

# Prepare insert query
insert_query = f"""
INSERT IGNORE INTO {table_name} ({', '.join(columns)})
VALUES ({', '.join(['%s'] * len(columns))})
"""

# Convert NaNs to None for SQL compatibility
data = dawn_df.where(pd.notnull(dawn_df), None).values.tolist()

# ✅ Improved performance: Insert data in batches
batch_size = 1000
for i in range(0, len(data), batch_size):
    cursor.executemany(insert_query, data[i:i + batch_size])
    conn.commit()
    print(f"✅ Inserted {i + batch_size} rows...")

print("🎉 All records inserted successfully.")

# Close database connection
cursor.close()
conn.close()


# In[ ]:





# #  Loading Data to Unified Database

# # Automation and Scheduling 

# In[ ]:


# Install the apache-airflow package
# %pip install apache-airflow

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# In[ ]:


def extract():
    print("Extracting data...")

def transform():
    print("Transforming data...")

def load():
    print("Loading data into database...")

dag = DAG(
    'data_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
)

task_extract = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
task_transform = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
task_load = PythonOperator(task_id='load', python_callable=load, dag=dag)

task_extract >> task_transform >> task_load


# # Automated ETL pipline using Airflow
