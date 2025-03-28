from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import requests
from pathlib import Path
import uuid
from datetime import timedelta, datetime
import logging
import shutil

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_pdf_file(file_path, target_uuid):
    base_api_endpoint = "http://vector:8000/vector/pdf/"
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    pdf_file = os.path.basename(file_path)
    api_endpoint = f"{base_api_endpoint}{target_uuid}/{pdf_file}"
    
    base_path = os.path.join("/appz/data/vector_watch_file_pdf/", target_uuid)
    path_parts = Path(file_path).relative_to(base_path).parts
    tags = list(path_parts[:-1]) if len(path_parts) > 1 else []
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(base_path, 'archive', timestamp)
    os.makedirs(archive_path, exist_ok=True)
    
    files = {'file': (pdf_file, open(file_path, 'rb'), 'application/pdf')}
    params = {'tags': ','.join(tags)} if tags else {}
    
    try:
        response = requests.post(api_endpoint, files=files, params=params)
        response.raise_for_status()
        logger.info(f"Successfully uploaded {pdf_file} with tags: {tags}")
        
        archive_destination = os.path.join(archive_path, pdf_file)
        shutil.move(file_path, archive_destination)
        logger.info(f"Archived {pdf_file} to {archive_destination}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error uploading {pdf_file}: {str(e)}")
        raise
    finally:
        files['file'][1].close()

with DAG(
    'shared_process_file_pdf2vector',
    default_args=default_args,
    description='Process PDF files to vector API in parallel',
    schedule_interval=None,  # Triggered DAG
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=50,  # Allow up to 50 simultaneous DAG runs
    concurrency=50,      # Allow up to 50 tasks to run concurrently
    params={
        'uuid': None,
        'file_path': None
    }
) as dag:

    def process_pdf_wrapper(**kwargs):
        conf = kwargs['dag_run'].conf
        if not conf or 'uuid' not in conf or 'file_path' not in conf:
            logger.error("Invalid configuration provided")
            raise ValueError("Missing required configuration")
        
        process_pdf_file(conf['file_path'], conf['uuid'])

    process_task = PythonOperator(
        task_id='process_pdf',
        python_callable=process_pdf_wrapper,
        execution_timeout=timedelta(minutes=30),  # Optional: timeout for each task
    )

    process_task