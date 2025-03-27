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

# Configure logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'shared_process_file_pdf2vector',
    default_args=default_args,
    description='Process individual PDF files to vector API',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        'uuid': None,
        'file_path': None
    }
)

def process_single_pdf(**kwargs):
    base_api_endpoint = "http://vector:8000/vector/pdf/"
    
    # Get parameters
    target_uuid = kwargs['params'].get('uuid')
    file_path = kwargs['params'].get('file_path')
    
    if not target_uuid or not file_path:
        logger.error("Missing required parameters (uuid or file_path)")
        return
    
    try:
        uuid.UUID(target_uuid)  # Validate UUID
    except ValueError:
        logger.error(f"Invalid UUID provided: {target_uuid}")
        return

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    pdf_file = os.path.basename(file_path)
    api_endpoint = f"{base_api_endpoint}{target_uuid}/{pdf_file}"
    
    # Extract tags from path
    base_path = f"/appz/data/vector_watch_file_pdf/{target_uuid}"
    path_parts = Path(file_path).relative_to(base_path).parts
    tags = list(path_parts[:-1]) if len(path_parts) > 1 else []
    
    # Create archive folder with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(base_path, 'archive', timestamp)
    os.makedirs(archive_path, exist_ok=True)
    
    # Prepare the upload data
    files = {'file': (pdf_file, open(file_path, 'rb'), 'application/pdf')}
    params = {'tags': ','.join(tags)} if tags else {}
    
    try:
        # Make the API call using POST
        response = requests.post(api_endpoint, files=files, params=params)
        response.raise_for_status()
        logger.info(f"Successfully uploaded {pdf_file} to {api_endpoint} with tags: {tags}")
        
        # Move to archive after successful upload
        archive_destination = os.path.join(archive_path, pdf_file)
        shutil.move(file_path, archive_destination)
        logger.info(f"Archived {pdf_file} to {archive_destination}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error uploading {pdf_file} to {api_endpoint}: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing {pdf_file}: {str(e)}")
    finally:
        files['file'][1].close()

# Define the task
process_task = PythonOperator(
    task_id='process_pdf_to_vector',
    python_callable=process_single_pdf,
    dag=dag,
)

process_task