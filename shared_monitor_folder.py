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
    'retries': 1,
}

# Function to log detected files with error handling
def log_detected_files(**context):
    try:
        file_path = context['task_instance'].xcom_pull(task_ids='watch_for_pdfs')
        run_id = str(uuid.uuid4())
        if file_path:
            logger.info(f"New PDF file detected: {file_path}")
            logger.info(f"Generated run_id: {run_id}")
        else:
            logger.info("No new PDF files detected")
    except Exception as e:
        logger.error(f"Error in log_detected_files: {str(e)}")
        raise

# DAG definition with error handling
try:
    with DAG(
        'shared_monitor_folder_pdf',
        default_args=default_args,
        description='Monitors a folder for new PDF files and logs detection',
        schedule_interval='* * * * *',
        start_date=days_ago(1),
        catchup=False,
    ) as dag:

        # FileSensor to monitor the folder (fs_conn_id removed)
        watch_folder = FileSensor(
            task_id='watch_for_pdfs',
            filepath='/appz/data/vector_watch_file_pdf',
            fs_conn_id='fs_default',
            poke_interval=60,
            timeout=3600,
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

except Exception as e:
    logger.error(f"Failed to initialize DAG pdf_file_watcher: {str(e)}")
    raise

logger.info("DAG pdf_file_watcher loaded successfully")