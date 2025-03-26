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

# DAG definition - removed schedule_interval
dag = DAG(
    'shared_process_file_pdf2vector',
    default_args=default_args,
    description='Upload PDFs to vector API with folder-based tags',
    schedule_interval=None,  # Set to None since we have required params
    start_date=days_ago(1),
    catchup=False,
    params={
        'uuid': None  # Optional UUID parameter
    }
)

def upload_pdfs(**kwargs):
    """
    Function to scan directory and upload PDFs to new endpoint format /pdf/{uuid}/{file_name}
    with archive functionality
    """
    base_path = "/appz/data/vector_watch_file_pdf/"
    base_api_endpoint = "http://vector:8000/vector/pdf/"
    
    # Get UUID from params
    target_uuid = kwargs['params'].get('uuid')
    if not target_uuid:
        logger.info("No UUID provided - stopping DAG")
        return
    
    try:
        uuid.UUID(target_uuid)  # Validate UUID
    except ValueError:
        logger.error(f"Invalid UUID provided: {target_uuid} - stopping DAG")
        return

    target_path = os.path.join(base_path, target_uuid)
    if not os.path.exists(target_path):
        logger.info(f"UUID directory {target_uuid} not found - stopping DAG")
        return

    # Scan all subdirectories recursively, excluding archive
    pdf_files_found = []
    for root, dirs, files in os.walk(target_path):
        # Skip archive directory
        if 'archive' in dirs:
            dirs.remove('archive')
        if 'archive' in root.lower():
            continue
            
        pdf_files = [f for f in files if f.lower().endswith('.pdf')]
        for pdf_file in pdf_files:
            pdf_files_found.append(os.path.join(root, pdf_file))

    if not pdf_files_found:
        logger.info(f"No PDF files found in UUID directory {target_uuid} - stopping DAG")
        return

    logger.info(f"Found {len(pdf_files_found)} PDF files in UUID directory: {target_uuid}")
    
    # Create archive folder with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(target_path, 'archive', timestamp)
    os.makedirs(archive_path, exist_ok=True)

    for file_path in pdf_files_found:
        pdf_file = os.path.basename(file_path)
        api_endpoint = f"{base_api_endpoint}{target_uuid}/{pdf_file}"
        
        # Extract tags from path
        path_parts = Path(file_path).relative_to(target_path).parts
        tags = list(path_parts[:-1]) if len(path_parts) > 1 else []
        
        # Prepare the upload data
        files = {'file': (pdf_file, open(file_path, 'rb'), 'application/pdf')}
        params = {'tags': ','.join(tags)} if tags else {}
        
        try:
            # Make the API call using POST
            response = requests.post(api_endpoint, files=files, params=params)
            response.raise_for_status()
            logger.info(f"Successfully uploaded {pdf_file} to {api_endpoint} with tags: {tags}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error uploading {pdf_file} to {api_endpoint}: {str(e)}")
            
        finally:
            # Close the file and move to archive
            files['file'][1].close()
            archive_destination = os.path.join(archive_path, pdf_file)
            try:
                shutil.move(file_path, archive_destination)
                logger.info(f"Archived {pdf_file} to {archive_destination}")
            except Exception as e:
                logger.error(f"Error archiving {pdf_file}: {str(e)}")

# Define the task
upload_task = PythonOperator(
    task_id='upload_pdfs_to_vector',
    python_callable=upload_pdfs,
    dag=dag,
)

upload_task
