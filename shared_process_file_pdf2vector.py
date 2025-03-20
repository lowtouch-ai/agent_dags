from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import requests
from pathlib import Path
import uuid
from datetime import timedelta
import logging
from airflow.exceptions import AirflowSkipException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    description='Upload PDFs to vector API with folder-based UUID',
    schedule_interval=timedelta(minutes=30),
    start_date=days_ago(1),
    catchup=False,
)

def check_and_upload_pdfs(**kwargs):
    """
    Function to check for PDFs in UUID folders and upload them to the vector API
    """
    base_path = "/appz/data/vector_watch_file_pdf/"
    # Update the base API endpoint (without uuid and file_name, as they will be added dynamically)
    api_base_url = "http://vector:8000/vector/pdf"
    
    # Counter for PDF files found
    pdf_count = 0
    has_valid_uuid = False
    
    # Walk through the directory structure
    for root, dirs, files in os.walk(base_path):
        # Check if the folder contains a valid UUID
        path_parts = Path(root).relative_to(base_path).parts
        if not path_parts:
            continue
            
        try:
            folder_uuid = uuid.UUID(path_parts[0])  # Validate UUID
            has_valid_uuid = True
        except ValueError:
            continue
            
        # Filter for PDF files only
        pdf_files = [f for f in files if f.endswith('.pdf')]
        pdf_count += len(pdf_files)
        
        if pdf_files:
            # Process each PDF file when found
            for pdf_file in pdf_files:
                file_path = os.path.join(root, pdf_file)
                
                # Construct the API endpoint with uuid and file_name
                api_endpoint = f"{api_base_url}/{folder_uuid}/{pdf_file}"
                
                # Prepare the upload data (only the file, no tags)
                files = {'file': (pdf_file, open(file_path, 'rb'), 'application/pdf')}
                
                try:
                    # Make the API call
                    response = requests.post(api_endpoint, files=files)
                    response.raise_for_status()
                    
                    logger.info(f"Successfully uploaded {pdf_file} to {api_endpoint}")
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Error uploading {pdf_file} to {api_endpoint}: {str(e)}")
                    
                finally:
                    # Close the file
                    files['file'][1].close()

    # Check conditions after scanning all folders
    if not has_valid_uuid:
        logger.info("No valid UUID folders found in the base path")
        raise AirflowSkipException("No valid UUID folders found, skipping DAG execution")
    
    if pdf_count == 0:
        logger.info("No PDF files found in UUID folders")
        raise AirflowSkipException("No PDF files found, skipping DAG execution")
    
    logger.info(f"Found and processed {pdf_count} PDF files")

# Define the task
upload_task = PythonOperator(
    task_id='check_and_upload_pdfs_to_vector',
    python_callable=check_and_upload_pdfs,
    dag=dag,
)

# Set task dependencies (if any)
upload_task