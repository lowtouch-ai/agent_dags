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

def process_pdf_file(**context):
    conf = context['dag_run'].conf
    file_path = conf.get('file_path')
    uuid = conf.get('uuid')
    tags = conf.get('tags', [])
    
    try:
        # Simulate PDF to vector processing
        logger.info(f"Processing PDF: {file_path} with tags: {tags}")
        
        # After processing, move to archive
        archive_path = os.path.join(
            '/appz/data/vector_watch_file_pdf', 
            uuid, 
            'archive'
        )
        os.makedirs(archive_path, exist_ok=True)
        
        file_name = os.path.basename(file_path)
        archive_file_path = os.path.join(archive_path, file_name)
        shutil.move(file_path, archive_file_path)
        logger.info(f"Moved processed file to: {archive_file_path}")
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        raise

with DAG(
    'shared_process_file_pdf2vector',
    default_args=default_args,
    description='Processes PDF files to vector format',
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "process", "pdf", "vector"],
    max_active_runs=50,      # Allow multiple parallel runs
    concurrency=50,         # Allow parallel task execution
    max_active_tasks=50     # Allow multiple files to process simultaneously
) as dag:

    process_task = PythonOperator(
        task_id='process_pdf_to_vector',
        python_callable=process_pdf_file,
        provide_context=True,
    )