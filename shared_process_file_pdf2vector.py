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
    description='Upload PDFs to vector API with folder-based tags',
    schedule_interval=None,  # Set to None since we have required params
    start_date=days_ago(1),
    catchup=False,
    params={
        'uuid': None,  # UUID parameter
        'pdf_file': None  # Specific PDF file parameter
    }
)

def upload_pdf(**kwargs):
    """
    Function to upload a specific PDF file to vector API
    """
    base_path = "/appz/data/vector_watch_file_pdf/"
    base_api_endpoint = "http://vector:8000/vector/pdf/"
    
    # Get UUID and PDF file from params
    target_uuid = kwargs['params'].get('uuid')
    target_pdf_file = kwargs['params'].get('pdf_file')
    
    if not target_uuid or not target_pdf_file:
        logger.info("No UUID or PDF file provided - stopping DAG")
        return
    
    try:
        uuid.UUID(target_uuid)  # Validate UUID
    except ValueError:
        logger.error(f"Invalid UUID provided: {target_uuid} - stopping DAG")
        return

    # Validate PDF file exists
    if not os.path.exists(target_pdf_file):
        logger.error(f"PDF file not found: {target_pdf_file}")
        return

    # Create archive folder with timestamp
    target_path = os.path.join(base_path, target_uuid)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(target_path, 'archive', timestamp)
    os.makedirs(archive_path, exist_ok=True)

    pdf_file = os.path.basename(target_pdf_file)
    api_endpoint = f"{base_api_endpoint}{target_uuid}/{pdf_file}"
    
    # Extract tags from path
    relative_path = Path(target_pdf_file).relative_to(target_path)
    tags = list(relative_path.parts[:-1]) if len(relative_path.parts) > 1 else []
    
    try:
        # Prepare the upload data
        with open(target_pdf_file, 'rb') as file:
            files = {'file': (pdf_file, file, 'application/pdf')}
            params = {'tags': ','.join(tags)} if tags else {}
            
            # Make the API call using POST
            response = requests.post(api_endpoint, files=files, params=params)
            response.raise_for_status()
            logger.info(f"Successfully uploaded {pdf_file} to {api_endpoint} with tags: {tags}")
        
        # Move file to archive
        archive_destination = os.path.join(archive_path, pdf_file)
        shutil.move(target_pdf_file, archive_destination)
        logger.info(f"Archived {pdf_file} to {archive_destination}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error uploading {pdf_file} to {api_endpoint}: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing {pdf_file}: {str(e)}")

# Define the task
upload_task = PythonOperator(
    task_id='upload_pdf_to_vector',
    python_callable=upload_pdf,
    dag=dag,
)

upload_task