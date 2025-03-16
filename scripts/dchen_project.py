import mysql.connector
import pandas as pd

# Function to connect to the database
def connect_to_db(database_name):
    db_config = {
        "host": "mysqldchen.mysql.database.azure.com",
        "user": "dchenAdmin",
        "password": "507password!",
        "database": database_name,
        "port": 3306
    }
    return mysql.connector.connect(**db_config)

# Function to load CSV file into a DataFrame
def load_csv(file_path):
    df = pd.read_csv(file_path, low_memory=False)  # Avoids datatype guessing issues
    return df

# Function to create table with optimized column types
def create_table(conn, table_name, df):
    cursor = conn.cursor()

    # Define column types dynamically (optimize row size)
    column_definitions = []
    for col in df.columns:
        if df[col].dtype == 'int64':
            col_type = "INT"
        elif df[col].dtype == 'float64':
            col_type = "FLOAT"
        elif df[col].nunique() < 255:  # If column has few unique values, use VARCHAR
            col_type = "VARCHAR(255)"
        else:
            col_type = "TEXT"  # Use TEXT only when necessary
        
        column_definitions.append(f"`{col}` {col_type}")
    
    columns_str = ",\n    ".join(column_definitions)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {columns_str}
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
    """

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()

# Function to insert data into MySQL
def insert_data(conn, table_name, df):
    cursor = conn.cursor()

    # Prepare insert query
    columns = ", ".join([f"`{col}`" for col in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

    # Convert DataFrame rows to list of tuples
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    try:
        cursor.executemany(insert_query, data)
        conn.commit()
    except mysql.connector.Error as e:
        print("Error inserting data:", e)

    cursor.close()

# Main execution
db_name = "icpsr_03088"
table_name = "adss_data_part1"
csv_file_path = "C:\\Users\\darre\\OneDrive\\Documents\\ADS\\ADS 507 Data Engineering\\data\\ICPSR_03088\\DS0001\\03088-0001-Data.csv" 

# Connect to MySQL database
conn = connect_to_db(db_name)

# Load CSV data
df = load_csv(csv_file_path)

# Create optimized table
create_table(conn, table_name, df)

# Insert data
insert_data(conn, table_name, df)

# Close connection
conn.close()

print("Data successfully loaded into MySQL database.")
