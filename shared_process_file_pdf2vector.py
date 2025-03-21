from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
import os
import requests
from pathlib import Path
import uuid
from datetime import timedelta
import logging

# Configure logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'shared_process_file_pdf2vector',
    default_args=default_args,
    description='Upload PDFs to vector API with folder-based tags',
    start_date=days_ago(1),
    catchup=False,
)

def upload_pdfs(**kwargs):
    """
    Function to scan directory and upload PDFs with appropriate tags
    Includes UUID validation and file presence check
    """
    base_path = "/appz/data/vector_watch_file_pdf/"
    # Using environment variable or explicit host name resolution
    api_endpoint = os.getenv('VECTOR_API_ENDPOINT', 'http://vector:8000/vector/pdf')
    
    # Verify API endpoint availability
    try:
        response = requests.get(api_endpoint, timeout=5)
        logger.info(f"API endpoint check response: {response.status_code}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Cannot connect to API endpoint {api_endpoint}: {str(e)}")
        raise

    # Get all immediate subdirectories
    subdirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
    
    if not subdirs:
        logger.info("No subdirectories found in base path")
        raise AirflowSkipException("No subdirectories found - skipping DAG execution")
    
    found_valid_files = False
    
    for subdir in subdirs:
        full_path = os.path.join(base_path, subdir)
        
        # Validate UUID
        try:
            uuid.UUID(subdir)
        except ValueError:
            logger.info(f"Skipping directory {subdir} - not a valid UUID")
            continue
            
        # Check for PDF files
        pdf_files = [f for f in os.listdir(full_path) if f.lower().endswith('.pdf')]
        
        if not pdf_files:
            logger.info(f"No PDF files found in UUID directory: {subdir}")
            continue
            
        logger.info(f"PDF files found in UUID directory {subdir}: {pdf_files}")
        found_valid_files = True
        
        # Extract tags from path
        path_parts = Path(full_path).relative_to(base_path).parts
        tags = list(path_parts[1:]) if len(path_parts) > 1 else []
        
        # Process each PDF file
        for pdf_file in pdf_files:
            file_path = os.path.join(full_path, pdf_file)
            
            # Prepare the upload data with proper file handling
            try:
                with open(file_path, 'rb') as f:
                    files = {'file': (pdf_file, f, 'application/pdf')}
                    data = {'tags': ','.join(tags)} if tags else {}
                    
                    # Make the API call with timeout
                    response = requests.post(
                        api_endpoint,
                        files=files,
                        data=data,
                        timeout=30
                    )
                    response.raise_for_status()
                    
                    logger.info(f"Successfully uploaded {pdf_file} with tags: {tags}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Error uploading {pdf_file}: {str(e)}")
            except IOError as e:
                logger.error(f"Error reading file {pdf_file}: {str(e)}")
    
    if not found_valid_files:
        logger.info("No valid PDF files found in any UUID directory")
        raise AirflowSkipException("No PDF files found - skipping DAG execution")

# Define the task
upload_task = PythonOperator(
    task_id='upload_pdfs_to_vector',
    python_callable=upload_pdfs,
    dag=dag,
)

# Set task dependencies
upload_task