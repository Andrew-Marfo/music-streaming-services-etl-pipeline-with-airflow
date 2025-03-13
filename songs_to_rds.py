import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("HOST")
DB_USER = os.getenv("USER")
DB_PASSWORD = os.getenv("PASSWORD")
DB_NAME = os.getenv("DATABASE")
DB_PORT = os.getenv("DB_PORT", "3306")

# Load CSV data
csv_file_path = "data/data/songs/songs.csv"
df = pd.read_csv(csv_file_path)

# print null values
print(df.isnull().sum())
# print(df.isnull().sum(axis=1))

print("Rows with null values:")
for index, row in df.iterrows():
    if row.isnull().any():
        print(f"Row {index}:")
        print(row)
        print("-" * 30)  

preprocessed_df = df.fillna(value="Unkown")

print(preprocessed_df.isnull().sum())

# Establish connection to the database
try:
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print("Connected to the database.")

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS songs (
        id INT PRIMARY KEY,
        track_id VARCHAR(100),
        artists VARCHAR(255),
        album_name VARCHAR(255),
        track_name VARCHAR(255),
        popularity INT,
        duration_ms INT,
        explicit BOOLEAN,
        danceability FLOAT,
        energy FLOAT,
        song_key INT,
        loudness FLOAT,
        mode INT,
        speechiness FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        liveness FLOAT,
        valence FLOAT,
        tempo FLOAT,
        time_signature INT,
        track_genre VARCHAR(100)
    )
    """
    cursor.execute(create_table_query)
    print("Table 'songs' created successfully.")

    # Insert data into the table using batch insert
    insert_query = """
    INSERT INTO songs (
        id, track_id, artists, album_name, track_name, popularity,
        duration_ms, explicit, danceability, energy, song_key,
        loudness, mode, speechiness, acousticness, instrumentalness,
        liveness, valence, tempo, time_signature, track_genre
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data_tuples = [tuple(row) for row in preprocessed_df.to_numpy()]

    batch_size = 5000
    for i in range(0, len(data_tuples), batch_size):
        cursor.executemany(insert_query, data_tuples[i:i + batch_size])
        conn.commit()
        print(f"Inserted {i + batch_size} rows into the 'songs' table.")
    print("Data successfully inserted into the 'songs' table.")

except Error as e:
    print(f"Error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
