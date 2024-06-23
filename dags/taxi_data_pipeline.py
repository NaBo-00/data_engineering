from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import logging
from create_visualization import main as visualize
import re

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Function to create database if it does not exist
def create_database_if_not_exists(db_name, user, password, host, port):
    import psycopg2
    conn = psycopg2.connect(
        dbname="postgres", user=user, password=password, host=host, port=port
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(f'CREATE DATABASE "{db_name}"')
        print(f"Database {db_name} created successfully.")
    else:
        print(f"Database {db_name} already exists.")
    cursor.close()
    conn.close()

# Function to get the latest file from the directory
def get_latest_file(directory, file_extension):
    files = [f for f in os.listdir(directory) if f.endswith(file_extension)]
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return files[0] if files else None

# Function to validate the file name format
def validate_file_name(file_name):
    # Define the regex pattern
    pattern = r"^yellow_tripdata_\d{4}_\d{2}$"
    
    # Check if the file name matches the pattern
    if not re.match(pattern, file_name):
        raise ValueError(f"File name '{file_name}' has an incorrect format. Expected format: yellow_tripdata_YYYY_MM")
    else:
        print(f"File name '{file_name}' is valid.")

# Function to decide whether to proceed or exit based on file presence and existing tables
def check_for_new_file_and_existing_tables(**context):
    directory = context['params']['directory']
    file_extension = context['params']['file_extension']
    latest_file = get_latest_file(directory, file_extension)
    print(f"Latest File Name: {latest_file}")

    if latest_file:
        latest_file_name = os.path.splitext(latest_file)[0]

        try:
            validate_file_name(latest_file_name)
        except ValueError as e:
            logging.error(e)
            return 'exit_pipeline'

        context['ti'].xcom_push(key='latest_file', value=latest_file_name)

        # Check if tables already exist
        raw_table_exists = table_exists('taxi_data_raw', f'{latest_file_name}_raw')
        processed_table_exists = table_exists('taxi_data_processed', f'{latest_file_name}_processed')
        
        if raw_table_exists and processed_table_exists:
            logging.info(f"Tables {latest_file_name}_raw and {latest_file_name}_processed already exist.")
            return 'exit_pipeline'
        else:
            return 'create_raw_database'
    else:
        logging.info(f"No new file detected in {directory}")
        return 'exit_pipeline'

# Function to check if a table exists in a database
def table_exists(database, table_name):
    import psycopg2
    try:
        conn = psycopg2.connect(
            dbname=database, user=os.getenv('POSTGRES_USER'), password=os.getenv('POSTGRES_PASSWORD'), host='postgres', port='5432' #'airflow' #'airflow' 
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname='public' AND tablename='{table_name}'")
        exists = cursor.fetchone()
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        logging.error(f"Error checking table existence: {e}")
        return False

# Dummy function to log exit
def exit_pipeline():
    logging.info("Exiting pipeline as no new file was detected.")

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'taxi_data_pipeline',
    default_args=default_args,
    description='A DAG to ingest, process, and store taxi trip data',
    schedule_interval='@monthly',  # Production: '@monthly' Testing: Run daily with '@daily', use customized CRON job notation, or trigger DAG manually
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    wait_for_file = BranchPythonOperator(
        task_id='wait_for_new_file',
        python_callable=check_for_new_file_and_existing_tables,
        params={
            'directory': '/data/',
            'file_extension': '.csv',
        },
        provide_context=True,
    )

    create_raw_db = PythonOperator(
        task_id='create_raw_database',
        python_callable=create_database_if_not_exists,
        op_kwargs={
            'db_name': 'taxi_data_raw',
            'user':  os.getenv('POSTGRES_USER'), #'airflow',
            'password':  os.getenv('POSTGRES_PASSWORD'), #'airflow',
            'host': 'postgres',
            'port': '5432',
        },
    )

    create_processed_db = PythonOperator(
        task_id='create_processed_database',
        python_callable=create_database_if_not_exists,
        op_kwargs={
            'db_name': 'taxi_data_processed',
            'user':  os.getenv('POSTGRES_USER'), #'airflow',
            'password':  os.getenv('POSTGRES_PASSWORD'), #'airflow',
            'host': 'postgres',
            'port': '5432',
        },
    )

    load_csv_to_raw = SparkSubmitOperator(
        task_id='load_csv_to_raw_db',
        application='/opt/spark/processing/load_raw_data.py',
        conn_id='spark_default',
        verbose=True,
        driver_memory='4g',
        executor_memory='4g',
        executor_cores=2,
        num_executors=4,
        application_args=['{{ ti.xcom_pull(key="latest_file", task_ids="wait_for_new_file") }}'],
        jars='/opt/spark/jars/postgresql-42.7.3.jar',
    )

    process_and_store_data = SparkSubmitOperator(
        task_id='process_data_and_store',
        application='/opt/spark/processing/process_data.py',
        conn_id='spark_default',
        verbose=True,
        driver_memory='4g',
        executor_memory='4g',
        executor_cores=2,
        num_executors=4,
        application_args=['{{ ti.xcom_pull(key="latest_file", task_ids="wait_for_new_file") }}'],
        jars='/opt/spark/jars/postgresql-42.7.3.jar',
    )

    visualize_data = PythonOperator(
        task_id='visualize_data',
        python_callable=visualize, 
        op_args=['{{ ti.xcom_pull(key="latest_file", task_ids="wait_for_new_file") }}'],
        dag=dag,
    )

    exit_pipeline_task = PythonOperator(
        task_id='exit_pipeline',
        python_callable=exit_pipeline,
    )

    # Define task dependencies
    wait_for_file >> [create_raw_db, exit_pipeline_task]
    create_raw_db >> create_processed_db >> load_csv_to_raw >> process_and_store_data >> visualize_data
     