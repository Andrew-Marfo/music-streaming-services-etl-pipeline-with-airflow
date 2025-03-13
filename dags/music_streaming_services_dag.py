from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
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
    return 'transform_data'  # Proceed to the next task if all validations succeed

# Transformation task (placeholder)
def transform_data(**kwargs):
    print("Transforming data...")
    # Add your transformation logic here


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

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Define task dependencies
validate_streams_in_s3_task >> extract_and_combine_streams_task
validate_streams_in_s3_task >> end_dag_if_no_streams_exists_task

extract_and_combine_streams_task >> validate_columns_task
extract_users_and_songs_from_postgres_task >> validate_columns_task

validate_columns_task >> transform_data_task 
validate_columns_task >> end_dag_if_columns_missing_task
