from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import os
import uuid
import logging

# Set up logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}
# Function to log detected files
def log_detected_files(**context):
    file_path = context['task_instance'].xcom_pull(task_ids='watch_for_pdfs')
    run_id = str(uuid.uuid4())
    if file_path:
        logger.info(f"New PDF file detected: {file_path}")
        logger.info(f"Generated run_id: {run_id}")
    else:
        logger.info("No new PDF files detected")

# DAG definition
with DAG(
    'pdf_file_watcher',
    default_args=default_args,
    description='Monitors a folder for new PDF files and logs detection',
    schedule_interval='@hourly',  # Adjust frequency as needed
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # FileSensor to monitor the folder
    watch_folder = FileSensor(
        task_id='watch_for_pdfs',
        filepath='/appz/data/vector_watch_file_pdf/*/*.pdf',  # Wildcard for UUID subfolders
        fs_conn_id='fs_default',  # Define this connection in Airflow admin
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # Timeout after 1 hour if no files found
        mode='poke',
    )

    # PythonOperator to log the detection
    log_detection = PythonOperator(
        task_id='log_pdf_detection',
        python_callable=log_detected_files,
        provide_context=True,
    )

    # Task dependency
    watch_folder >> log_detection
