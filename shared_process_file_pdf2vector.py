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

dag = DAG(
    'shared_process_file_pdf2vector',
    default_args=default_args,
    description='Upload PDFs to vector API with folder-based tags',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        'uuid': None,
        'file_path': None,
        'file_name': None
    }
)

def upload_pdf(**kwargs):
    base_api_endpoint = "http://vector:8000/vector/pdf/"
    
    # Get parameters
    target_uuid = kwargs['params'].get('uuid')
    file_path = kwargs['params'].get('file_path')
    file_name = kwargs['params'].get('file_name')
    
    if not all([target_uuid, file_path, file_name]):
        logger.error("Missing required parameters")
        return
    
    try:
        uuid.UUID(target_uuid)  # Validate UUID
    except ValueError:
        logger.error(f"Invalid UUID provided: {target_uuid}")
        return

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    # Prepare API endpoint
    api_endpoint = f"{base_api_endpoint}{target_uuid}/{file_name}"
    
    # Extract tags from path relative to UUID directory
    base_path = f"/appz/data/vector_watch_file_pdf/{target_uuid}"
    path_parts = Path(file_path).relative_to(base_path).parts
    tags = list(path_parts[:-1]) if len(path_parts) > 1 else []
    
    # Create archive folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(base_path, 'archive', timestamp)
    os.makedirs(archive_path, exist_ok=True)
    
    # Upload file
    files = {'file': (file_name, open(file_path, 'rb'), 'application/pdf')}
    params = {'tags': ','.join(tags)} if tags else {}
    
    try:
        response = requests.post(api_endpoint, files=files, params=params)
        response.raise_for_status()
        logger.info(f"Successfully uploaded {file_name} with tags: {tags}")
        
        # Archive file
        archive_destination = os.path.join(archive_path, file_name)
        shutil.move(file_path, archive_destination)
        logger.info(f"Archived {file_name} to {archive_destination}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error uploading {file_name}: {str(e)}")
    except Exception as e:
        logger.error(f"Error archiving {file_name}: {str(e)}")
    finally:
        files['file'][1].close()

upload_task = PythonOperator(
    task_id='upload_pdf_to_vector',
    python_callable=upload_pdf,
    dag=dag,
)

upload_task