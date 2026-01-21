from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import requests
from pathlib import Path
import logging
import shutil
from datetime import timedelta, datetime

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'lowtouch.ai_developers',
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'retries': 1,
    "retry_delay": timedelta(seconds=15),
}

def process_pdf_file(**context):
    conf = context['dag_run'].conf
    file_path = conf.get('file_path')
    target_uuid = conf.get('uuid')
    base_api_endpoint = "http://vector:8000/vector/pdf/"
    tags = conf.get('tags', [])
    pdf_file = os.path.basename(file_path)
    api_endpoint = f"{base_api_endpoint}{target_uuid}/{pdf_file}"
    
    base_path = os.path.join("/appz/data/vector_watch_file_pdf/", target_uuid)
    
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
    description='Processes PDF files to vector format',
    schedule=None,
    catchup=False,
    tags=["shared", "process", "pdf", "vector"],
    max_active_runs=6
) as dag:

    process_task = PythonOperator(
        task_id='process_pdf_to_vector',
        python_callable=process_pdf_file,
        provide_context=True,
    )
