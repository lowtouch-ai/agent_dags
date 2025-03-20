from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import requests
from pathlib import Path
import uuid
from datetime import timedelta

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
    schedule_interval=timedelta(minutes=30),  # Runs every 30 minutes
    start_date=days_ago(1),
    catchup=False,
)

def upload_pdfs(**kwargs):
    """
    Function to scan directory and upload PDFs with appropriate tags
    """
    base_path = "/appz/data/vector_watch_file_pdf"
    api_endpoint = "http://vector:8000/vector/pdf"
    
    # Walk through the directory structure
    for root, dirs, files in os.walk(base_path):
        # Filter for PDF files only
        pdf_files = [f for f in files if f.endswith('.pdf')]
        
        if not pdf_files:
            continue
            
        # Extract UUID and tags from path
        path_parts = Path(root).relative_to(base_path).parts
        if not path_parts:
            continue
            
        try:
            # First part should be UUID
            uuid.UUID(path_parts[0])
            # Remaining parts are tags
            tags = list(path_parts[1:]) if len(path_parts) > 1 else []
        except ValueError:
            # Skip if no valid UUID found
            continue
            
        # Process each PDF file
        for pdf_file in pdf_files:
            file_path = os.path.join(root, pdf_file)
            
            # Prepare the upload data
            files = {'file': (pdf_file, open(file_path, 'rb'), 'application/pdf')}
            data = {'tags': ','.join(tags)} if tags else {}
            
            try:
                # Make the API call
                response = requests.post(api_endpoint, files=files, data=data)
                response.raise_for_status()
                
                print(f"Successfully uploaded {pdf_file} with tags: {tags}")
                
            except requests.exceptions.RequestException as e:
                print(f"Error uploading {pdf_file}: {str(e)}")
                # Consider adding retry logic or error handling based on your needs
                
            finally:
                # Close the file
                files['file'][1].close()

# Define the task
upload_task = PythonOperator(
    task_id='upload_pdfs_to_vector',
    python_callable=upload_pdfs,
    dag=dag,
)

# Set task dependencies (if any)
upload_task
