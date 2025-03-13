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
def check_s3_bucket(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Uses AWS connection stored in Airflow UI
    bucket_name = 'streaming-data-nsp24'
    objects = s3_hook.list_keys(bucket_name=bucket_name)
    
    if objects:
        kwargs['ti'].xcom_push(key='files_exist', value=True)
    else:
        kwargs['ti'].xcom_push(key='files_exist', value=False)

# Decides the next task based on the files in the S3 bucket
def decide_next_task(**kwargs):
    ti = kwargs['ti']
    files_exist = ti.xcom_pull(task_ids='check_s3_bucket', key='files_exist')
    
    return 'extract_and_combine_files' if files_exist else 'end_dag_if_no_files_in_s3'

# Extracts and combines the files from the S3 bucket
def extract_and_combine_files(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'streaming-data-nsp24'
    objects = s3_hook.list_keys(bucket_name=bucket_name)
    
    combined_data = []
    for file_key in objects:
        file_content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)
        combined_data.append(file_content)
    
    combined_file_path = '/tmp/combined_streaming_data.csv'
    with open(combined_file_path, 'w') as f:
        f.write('\n'.join(combined_data))
    
    kwargs['ti'].xcom_push(key='combined_file_path', value=combined_file_path)

# Validates if all required columns are present in the combined data
def validate_columns(**kwargs):
    ti = kwargs['ti']
    combined_file_path = ti.xcom_pull(task_ids='extract_and_combine_files', key='combined_file_path')
    
    # Read the combined file into a DataFrame
    df = pd.read_csv(combined_file_path)
    
    # Define the required columns
    required_columns = {'user_id', 'track_id', 'listen_time'}
    
    # Check if all required columns are present
    if not required_columns.issubset(df.columns):
        missing_columns = required_columns - set(df.columns)
        print(f"Missing required columns: {missing_columns}")
        return 'end_dag'  # End the DAG if columns are missing
    
    # If all columns are present, log a success message
    print("All required columns are present in the combined data.")
    return 'transform_data'  # Proceed to the next task if columns are present

# Extracting songs and users data from postgres
def extract_users_and_songs_from_postgres(**kwargs):

    # Establish connection to the database
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

# Validate if there are stream files in the S3 bucket task
check_s3_bucket_task = PythonOperator(
    task_id='check_s3_bucket',
    python_callable=check_s3_bucket,
    dag=dag,
)

# Branching based on the presence of files in the S3 bucket task
branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_next_task,
    dag=dag,
)

# Stopping the DAG if there are no files in the S3 bucket task
end_dag_task_if_no_files_exists = EmptyOperator(
    task_id='end_dag_if_no_files_in_s3',
    dag=dag,
)

# Extracting and combining streams from the S3 bucket task
extract_and_combine_streams_task = PythonOperator(
    task_id='extract_and_combine_streams',
    python_callable=extract_and_combine_files,
    dag=dag,
)

# Validate if all required columns are present in the combined stream data task
validate_columns_task = PythonOperator(
    task_id='validate_columns',
    python_callable=validate_columns,
    dag=dag,
)

# Branching based on the validation of columns task
branch_after_validation_task = BranchPythonOperator(
    task_id='branch_after_validation',
    python_callable=validate_columns,
    dag=dag,
)

# Stop the DAG if required columns are missing
end_dag_task = EmptyOperator(
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
check_s3_bucket_task >> branch_task
branch_task >> extract_and_combine_files_task
branch_task >> end_dag_task_if_no_files_exists
extract_and_combine_files_task >> validate_columns_task
validate_columns_task >> branch_after_validation_task
branch_after_validation_task >> transform_data_task
branch_after_validation_task >> end_dag_task

extract_users_and_songs_from_postgres_task >> transform_data_task