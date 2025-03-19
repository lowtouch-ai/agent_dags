from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import os
import logging

# Set up logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Function to check folder and log results
def check_and_log_files(**context):
    folder_path = '/appz/data/vector_watch_file_pdf'
    try:
        # Check if folder exists
        if not os.path.exists(folder_path):
            logger.error(f"Folder {folder_path} does not exist")
            raise Exception(f"Folder {folder_path} not found")
        
        # Get list of PDF files
        pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
        
        if not pdf_files:
            logger.info("No PDF files found in the monitored folder")
        else:
            logger.info(f"PDF file(s) found: {pdf_files}")
            
    except Exception as e:
        logger.error(f"Error in check_and_log_files: {str(e)}")
        raise

# DAG definition with error handling
try:
    with DAG(
        'shared_monitor_folder_pdf',
        default_args=default_args,
        description='Monitors a folder for PDF files and logs detection',
        schedule_interval='* * * * *',  # Runs every minute
        start_date=days_ago(1),
        catchup=False,
    ) as dag:

        # Start task
        start = DummyOperator(
            task_id='start'
        )

        # PythonOperator to check folder and log
        check_folder = PythonOperator(
            task_id='check_pdf_folder',
            python_callable=check_and_log_files,
            provide_context=True,
        )

        # End task
        end = DummyOperator(
            task_id='end'
        )

        # Task dependencies
        start >> check_folder >> end

except Exception as e:
    logger.error(f"Failed to initialize DAG shared_monitor_folder_pdf: {str(e)}")
    raise

logger.info("DAG shared_monitor_folder_pdf loaded successfully")