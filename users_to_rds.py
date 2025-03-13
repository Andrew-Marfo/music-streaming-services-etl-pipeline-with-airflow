import pandas as pd
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("HOST")
DB_USER = os.getenv("USER")
DB_PASSWORD = os.getenv("PASSWORD")
DB_NAME = os.getenv("DATABASE")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default PostgreSQL port is 5432

# Load CSV data
csv_file_path = "data/data/users/users.csv"
df = pd.read_csv(csv_file_path)

# Establish connection to the database
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print("Connected to the database.")

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        user_id SERIAL PRIMARY KEY,
        user_name TEXT,
        user_age INTEGER,
        user_country TEXT,
        created_at DATE
    )
    """
    cursor.execute(create_table_query)
    print("Table 'users' created successfully.")

    # Prepare data for batch insertion
    # Convert DataFrame rows to a list of tuples
    data_tuples = [tuple(row) for row in df.to_numpy()]

    # Insert data into the table using batch insertion
    insert_query = """
    INSERT INTO users (user_id, user_name, user_age, user_country, created_at)
    VALUES (%s, %s, %s, %s, %s)
    """
    
    batch_size = 1000
    for i in range(0, len(data_tuples), batch_size):
        cursor.executemany(insert_query, data_tuples[i:i + batch_size])
        conn.commit()
        print(f"Inserted {i + batch_size} rows into the 'users' table.")
    print("Data successfully inserted into the 'users' table.")

except Error as e:
    print(f"Error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()