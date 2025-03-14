from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import timedelta
import psycopg2
from psycopg2 import Error
import pandas as pd

# Validates if there are files in the S3 bucket
def validate_streams_in_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Uses AWS connection stored in Airflow UI
    bucket_name = 'streaming-data-nsp24'
    objects = s3_hook.list_keys(bucket_name=bucket_name)
    
    if objects:
        return 'extract_and_combine_streams'
    else:
        return 'end_dag_if_no_streams_exists_task'

# Extracts and combines the files from the S3 bucket
def extract_and_combine_streams(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'streaming-data-nsp24'
    objects = s3_hook.list_keys(bucket_name=bucket_name)
    
    combined_data = []
    for file_key in objects:
        file_content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)
        combined_data.append(file_content)
    
    print(f"Extracted {len(combined_data)} records from S3.")
    combined_file_path = '/tmp/combined_streaming_data.csv'
    with open(combined_file_path, 'w') as f:
        f.write('\n'.join(combined_data))
    
    kwargs['ti'].xcom_push(key='combined_file_path', value=combined_file_path)


# Extracting songs and users data from postgres
def extract_users_and_songs_from_postgres(**kwargs):

    # Establish connection to the database
    cursor = None
    try:
        # Get RDS connection details from Airflow Connections
        rds_conn = BaseHook.get_connection("aws_postgres_conn")
        conn = psycopg2.connect(
            host=rds_conn.host,
            user=rds_conn.login,
            password=rds_conn.password,
            dbname=rds_conn.schema,
            port=rds_conn.port
        )
        cursor = conn.cursor()
        print("Connected to the database.")

        # Extract users data
        print("Extracting users data...")
        users_query = "SELECT * FROM users"
        cursor.execute(users_query)
        users_rows = cursor.fetchall()
        users_df = pd.DataFrame(users_rows, columns=[desc[0] for desc in cursor.description])

        # Save users data to a temporary CSV file
        users_csv_path = "/tmp/users_data.csv"
        users_df.to_csv(users_csv_path, index=False)
        print(f"Users data extracted and saved to {users_csv_path}")

        # Push the users file path to XCom
        kwargs['ti'].xcom_push(key='users_csv_path', value=users_csv_path)

        # Extract songs data
        print("Extracting songs data...")
        songs_query = "SELECT * FROM songs"
        cursor.execute(songs_query)
        songs_rows = cursor.fetchall()
        songs_df = pd.DataFrame(songs_rows, columns=[desc[0] for desc in cursor.description])

        # Save songs data to a temporary CSV file
        songs_csv_path = "/tmp/songs_data.csv"
        songs_df.to_csv(songs_csv_path, index=False)
        print(f"Songs data extracted and saved to {songs_csv_path}")

        # Push the songs file path to XCom
        kwargs['ti'].xcom_push(key='songs_csv_path', value=songs_csv_path)

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Validates if all required columns are present in the combined stream data, users and songs
def validate_columns(**kwargs):
    ti = kwargs['ti']
    
    # Pull file paths from XCom
    combined_streams_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='combined_file_path')
    users_csv_path = ti.xcom_pull(task_ids='extract_users_and_songs_from_postgres', key='users_csv_path')
    songs_csv_path = ti.xcom_pull(task_ids='extract_users_and_songs_from_postgres', key='songs_csv_path')
    
    # Read the CSV files into DataFrames
    streams_df = pd.read_csv(combined_streams_path)
    users_df = pd.read_csv(users_csv_path)
    songs_df = pd.read_csv(songs_csv_path)
    
    # Define the required columns for each dataset
    required_columns_streams = {'user_id', 'track_id', 'listen_time'}
    required_columns_users = {'user_id', 'user_name', 'user_age', 'user_country', 'created_at'}
    required_columns_songs = {
        'track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms', 'explicit',
        'danceability', 'energy', 'song_key', 'loudness', 'mode', 'speechiness', 'acousticness',
        'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature', 'track_genre'
    }
    
    # Validate streams data
    if not required_columns_streams.issubset(streams_df.columns):
        missing_columns = required_columns_streams - set(streams_df.columns)
        print(f"Missing required columns in streams data: {missing_columns}")
        return 'end_dag_if_columns_missing'  # End the DAG if streams validation fails
    
    # Validate users data
    if not required_columns_users.issubset(users_df.columns):
        missing_columns = required_columns_users - set(users_df.columns)
        print(f"Missing required columns in users data: {missing_columns}")
        return 'end_dag_if_columns_missing'  # End the DAG if users validation fails
    
    # Validate songs data
    if not required_columns_songs.issubset(songs_df.columns):
        missing_columns = required_columns_songs - set(songs_df.columns)
        print(f"Missing required columns in songs data: {missing_columns}")
        return 'end_dag_if_columns_missing'  # End the DAG if songs validation fails
    
    # If all validations pass, log a success message
    print("All required columns are present in streams, users, and songs data.")
    print(f"Extracted {len(streams_df)} records from S3.")
    print(f"Extracted {len(users_df)} records from Postgres.")
    print(f"Extracted {len(songs_df)} records from Postgres.")
    return 'transform_and_compute_kpis'  # Proceed to the next task if all validations succeed

# Transformation task (placeholder)
def transform_and_compute_kpis(**kwargs):
    print("Transforming data...")
    ti = kwargs['ti']
    
    # Pull file paths from XCom
    combined_streams_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='combined_file_path')
    users_csv_path = ti.xcom_pull(task_ids='extract_users_and_songs_from_postgres', key='users_csv_path')
    songs_csv_path = ti.xcom_pull(task_ids='extract_users_and_songs_from_postgres', key='songs_csv_path')
    
    # Read the CSV files into DataFrames
    streams_df = pd.read_csv(combined_streams_path)
    users_df = pd.read_csv(users_csv_path)
    songs_df = pd.read_csv(songs_csv_path)

    # Convert 'listen_time' to datetime as a string
    streams_df['listen_time'] = streams_df['listen_time'].astype(str)
    streams_df["listen_time"] = pd.to_datetime(streams_df["listen_time"], errors="coerce").astype(str)
    
    # Merge streams with songs to get genre information
    merged_df = pd.merge(streams_df, songs_df, left_on='track_id', right_on='track_id', how='left')
    
    # Extract date and hour from the listen_time column
    merged_df['date'] = pd.to_datetime(merged_df['listen_time']).dt.date
    merged_df['hour'] = pd.to_datetime(merged_df['listen_time']).dt.hour
    
    # Genre-Level KPIs (Daily Basis)
    genre_kpis = merged_df.groupby(['date', 'track_genre']).agg(
        listen_count=('track_id', 'count'),  # Total listen count per genre per day
        avg_duration_ms=('duration_ms', 'mean'),  # Average track duration per genre per day
        popularity_index=('popularity', 'mean')  # Average popularity index per genre per day
    ).reset_index()
    
    # Compute the most popular track per genre per day
    most_popular_track_per_genre_per_day = (
        merged_df.loc[merged_df.groupby(['date', 'track_genre'])['popularity'].idxmax()]
        [['date', 'track_genre', 'track_name', 'popularity']]
    )
    
    # Merge the most popular track information into the genre_kpis table
    genre_kpis = pd.merge(
        genre_kpis,
        most_popular_track_per_genre_per_day,
        on=['date', 'track_genre'],
        how='left'
    )
    
    # Rename columns for clarity
    genre_kpis.rename(columns={
        'track_name': 'most_popular_track',
        'popularity': 'most_popular_track_popularity'
    }, inplace=True)

    # Save Genre-Level KPIs to a CSV file
    genre_kpis_csv_path = '/tmp/genre_kpis_daily.csv'
    genre_kpis.to_csv(genre_kpis_csv_path, index=False)
    
    # Log the results
    print("Genre-Level KPIs computed and saved to CSV file.")
    print(f"Genre KPIs saved to: {genre_kpis_csv_path}")
    
    # Push the file path to XCom for use in downstream tasks
    ti.xcom_push(key='genre_kpis_csv_path', value=genre_kpis_csv_path)

    # Hourly KPIs (Daily Basis)
    hourly_kpis = merged_df.groupby(['date', 'hour']).agg(
        unique_listeners=('user_id', 'nunique'),  # Unique listeners per hour per day
        total_plays=('track_id', 'count'),  # Total plays per hour per day
        unique_tracks=('track_id', 'nunique')  # Unique tracks per hour per day
    ).reset_index()
    
    # Compute Track Diversity Index
    hourly_kpis['track_diversity_index'] = hourly_kpis['unique_tracks'] / hourly_kpis['total_plays']
    
    # Compute Top Artists per Hour
    top_artists_per_hour = (
        merged_df.groupby(['date', 'hour', 'artists'])['track_id']
        .count()
        .reset_index(name='play_count')
        .sort_values(['date', 'hour', 'play_count'], ascending=[True, True, False])
        .groupby(['date', 'hour'])
        .head(1)
        .rename(columns={'artists': 'top_artist', 'play_count': 'top_artist_plays'})
    )
    
    # Merge Top Artists into the hourly_kpis table
    hourly_kpis = pd.merge(
        hourly_kpis,
        top_artists_per_hour[['date', 'hour', 'top_artist']],  # Only include 'top_artist' column
        on=['date', 'hour'],
        how='left'
    )
    
    # Select only the required columns for the final hourly KPIs table
    hourly_kpis = hourly_kpis[['date', 'hour', 'unique_listeners', 'top_artist', 'track_diversity_index']]
    
    # Save Hourly KPIs to a CSV file
    hourly_kpis_csv_path = '/tmp/hourly_kpis_daily.csv'
    hourly_kpis.to_csv(hourly_kpis_csv_path, index=False)
    
    # Log the results
    print("Hourly KPIs computed and saved to CSV file.")
    print(f"Hourly KPIs saved to: {hourly_kpis_csv_path}")
    
    # Push the file path to XCom for use in downstream tasks
    ti.xcom_push(key='hourly_kpis_csv_path', value=hourly_kpis_csv_path)


# ------------------------------------------------------------------------------------------------

# Dag definition and task dependencies



# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'music_streaming_etl',
    default_args=default_args,
    description='ETL pipeline for music streaming data',
    schedule_interval=timedelta(days=1),
)

# Branching based on the presence of files in the S3 bucket task
validate_streams_in_s3_task = BranchPythonOperator(
    task_id='validating_streams_in_s3',
    python_callable=validate_streams_in_s3,
    dag=dag,
)

# Stopping the DAG if there are no files in the S3 bucket task
end_dag_if_no_streams_exists_task = EmptyOperator(
    task_id='end_dag_if_no_streams_exists_in_s3',
    dag=dag,
)

# Extracting and combining streams from the S3 bucket task
extract_and_combine_streams_task = PythonOperator(
    task_id='extract_and_combine_streams',
    python_callable=extract_and_combine_streams,
    dag=dag,
)


# Branching based on the validation of columns task
validate_columns_task = BranchPythonOperator(
    task_id='validate_columns',
    python_callable=validate_columns,
    dag=dag,
)

# Stop the DAG if required columns are missing
end_dag_if_columns_missing_task = EmptyOperator(
    task_id='end_dag_if_columns_missing',
    dag=dag,
)

# Extracting users and songs data from Postgres task
extract_users_and_songs_from_postgres_task = PythonOperator(
    task_id='extract_users_and_songs_from_postgres',
    python_callable=extract_users_and_songs_from_postgres,
    dag=dag,
)

# Transformation and KPI computation task
transform_and_compute_kpis_task = PythonOperator(
    task_id='transform_and_compute_kpis',
    python_callable=transform_and_compute_kpis,
    dag=dag,
)

# Task dependencies
validate_streams_in_s3_task >> extract_and_combine_streams_task
validate_streams_in_s3_task >> end_dag_if_no_streams_exists_task

extract_and_combine_streams_task >> validate_columns_task
extract_users_and_songs_from_postgres_task >> validate_columns_task

validate_columns_task >> transform_and_compute_kpis_task 
validate_columns_task >> end_dag_if_columns_missing_task

