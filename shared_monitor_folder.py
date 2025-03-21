from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import logging
import uuid as uuid_validator
import json

# Set up logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def is_valid_uuid(uuid_str):
    """Check if a string is a valid UUID"""
    try:
        uuid_obj = uuid_validator.UUID(uuid_str)
        return True
    except ValueError:
        return False

def find_pdf_files_in_uuid_dir(uuid_dir_path):
    """Find all PDF files in a UUID directory and its subdirectories, excluding the archive folder"""
    pdf_files = []
    
    for root, dirs, files in os.walk(uuid_dir_path):
        # Skip archive directory
        if 'archive' in dirs:
            dirs.remove('archive')
        
        # Check for PDF files in current directory
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    
    return pdf_files

def check_and_process_files(**context):
    """Check for UUID directories and PDF files, trigger processing DAG if found"""
    base_folder_path = '/appz/data/vector_watch_file_pdf'
    found_pdfs = {}
    processing_triggered = False
    
    try:
        # Check if base folder exists
        if not os.path.exists(base_folder_path):
            logger.error(f"Base folder {base_folder_path} does not exist")
            raise Exception(f"Base folder {base_folder_path} not found")
        
        # Get direct contents of the base folder
        base_contents = os.listdir(base_folder_path)
        
        # Check for PDF files in the base folder
        base_pdf_files = [f for f in base_contents if f.lower().endswith('.pdf')]
        if base_pdf_files:
            found_pdfs['base_folder'] = base_pdf_files
        
        # Check for UUID directories
        uuid_dirs = [d for d in base_contents if os.path.isdir(os.path.join(base_folder_path, d)) and is_valid_uuid(d)]
        
        # Process each UUID directory
        for uuid_dir in uuid_dirs:
            uuid_dir_path = os.path.join(base_folder_path, uuid_dir)
            pdf_files = find_pdf_files_in_uuid_dir(uuid_dir_path)
            
            if pdf_files:
                found_pdfs[uuid_dir] = pdf_files
                
                # Set XCom value for the UUID where PDF was found
                context['ti'].xcom_push(key=f'uuid_with_pdf', value=uuid_dir)
                processing_triggered = True
                
                logger.info(f"PDF files found in UUID directory {uuid_dir}: {pdf_files}")
                return uuid_dir  # Return the UUID to trigger the processing DAG
        
        # If no PDFs found anywhere, log and end DAG
        if not found_pdfs:
            logger.info("No PDF files found in any monitored locations")
            return None
            
        # If PDFs found only in base folder but not in UUID directories
        if not processing_triggered and 'base_folder' in found_pdfs:
            logger.info(f"PDF files found only in base folder: {found_pdfs['base_folder']}")
            return None
            
    except Exception as e:
        logger.error(f"Error in check_and_process_files: {str(e)}")
        raise
    
    return None

def decide_to_trigger_processing(**context):
    """Decide whether to trigger the processing DAG based on check results"""
    ti = context['ti']
    uuid_to_process = ti.xcom_pull(task_ids='check_pdf_folders')
    
    if uuid_to_process:
        return uuid_to_process
    else:
        logger.info("No eligible PDF files found for processing, skipping trigger")
        return None

# DAG definition with error handling
try:
    with DAG(
        'shared_monitor_folder_pdf',
        default_args=default_args,
        description='Monitors folder for PDF files in UUID directories and triggers processing',
        schedule_interval='* * * * *',  # Runs every minute
        start_date=days_ago(1),
        catchup=False,
    ) as dag:

        # Start task
        start = DummyOperator(
            task_id='start'
        )

        # PythonOperator to check folder and log
        check_folders = PythonOperator(
            task_id='check_pdf_folders',
            python_callable=check_and_process_files,
            provide_context=True,
        )

        # Decide whether to trigger processing DAG
        decide_trigger = PythonOperator(
            task_id='decide_trigger',
            python_callable=decide_to_trigger_processing,
            provide_context=True,
        )

        # Conditional trigger for the processing DAG
        trigger_processing = TriggerDagRunOperator(
            task_id='trigger_processing_dag',
            trigger_dag_id='shared_process_file_pdf2vector',
            conf=lambda context: {"uuid": context['ti'].xcom_pull(task_ids='check_pdf_folders')},
            trigger_run_id=None,  # Auto-generate a run ID
            execution_date=None,  # Use current execution date
            reset_dag_run=True,  # Reset if already exists
            wait_for_completion=False,  # Don't wait for completion
            poke_interval=10,  # Check every 10 seconds
            trigger_rule='none_failed_or_skipped',  # Run only if all upstream tasks succeed or are skipped
        )

        # End task
        end = DummyOperator(
            task_id='end'
        )

        # Task dependencies
        start >> check_folders >> decide_trigger
        decide_trigger >> trigger_processing >> end

except Exception as e:
    logger.error(f"Failed to initialize DAG shared_monitor_folder_pdf: {str(e)}")
    raise

logger.info("DAG shared_monitor_folder_pdf loaded successfully")