from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import logging
import shutil

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'retries': 1,
    "retry_delay": timedelta(seconds=15),
}

def process_pdf_to_vector(**context):
    conf = context['dag_run'].conf
    uuid = conf.get('uuid')
    file_path = conf.get('file_path')
    tags = conf.get('tags', [])
    
    try:
        logger.info(f"Processing PDF: {file_path}")
        logger.info(f"UUID: {uuid}")
        logger.info(f"Tags: {tags}")
        
        # Add your PDF to vector conversion logic here
        # This is a placeholder for the actual processing
        logger.info(f"Starting PDF to vector conversion for {file_path}")
        
        # Simulate processing
        # Replace this with actual PDF processing code
        archive_path = file_path.replace('processing_pdf', 'archive')
        os.makedirs(os.path.dirname(archive_path), exist_ok=True)
        
        # Move to archive after processing
        shutil.move(file_path, archive_path)
        logger.info(f"Completed processing and moved to {archive_path}")
        
    except Exception as e:
        logger.error(f"Error processing PDF {file_path}: {str(e)}")
        raise

with DAG(
    'shared_process_file_pdf2vector',
    default_args=default_args,
    description='Processes PDF files to vector format',
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "pdf", "vector", "processing", "rag"],
    max_active_runs=50,      # Allow multiple parallel runs
    concurrency=50,         # Allow parallel task execution
    max_active_tasks=50     # Allow multiple tasks to run simultaneously
) as dag:

    process_task = PythonOperator(
        task_id='process_pdf_to_vector',
        python_callable=process_pdf_to_vector,
        provide_context=True,
    )

    process_task

logger.info("DAG shared_process_file_pdf2vector loaded successfully")