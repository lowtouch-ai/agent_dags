from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import os
import logging
import re

# Set up logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'retries': 1,
    "retry_delay": timedelta(seconds=15),
}

# UUID regex pattern (matches standard UUID format: 8-4-4-4-12)
UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

def check_and_process_folders(**context):
    folder_path = '/appz/data/vector_watch_file_pdf'
    
    try:
        if not os.path.exists(folder_path):
            logger.error(f"Base folder {folder_path} does not exist")
            raise Exception(f"Folder {folder_path} not found")
        
        # Find UUID directories
        uuid_dirs = [
            dir_name for dir_name in os.listdir(folder_path)
            if UUID_PATTERN.match(dir_name) and 
            os.path.isdir(os.path.join(folder_path, dir_name))
        ]
        
        if not uuid_dirs:
            logger.info("No UUID directories found in the monitored folder")
            return []

        # Collect PDF files across all UUID directories
        pdf_files_by_uuid = {}
        for uuid_dir in uuid_dirs:
            uuid_path = os.path.join(folder_path, uuid_dir)
            pdf_files = []
            
            for root, dirs, files in os.walk(uuid_path):
                # Skip archive folder
                if 'archive' in dirs:
                    dirs.remove('archive')
                if 'archive' in root.lower():
                    continue
                    
                pdf_files.extend(
                    os.path.join(root, f) 
                    for f in files 
                    if f.lower().endswith('.pdf')
                )
            
            if pdf_files:
                pdf_files_by_uuid[uuid_dir] = pdf_files
        
        if not pdf_files_by_uuid:
            logger.info("No PDF files found in any UUID directories")
            return []
        
        # Log findings and prepare for processing
        for uuid, files in pdf_files_by_uuid.items():
            logger.info(f"Found {len(files)} PDF files in UUID {uuid}")
        
        return pdf_files_by_uuid

    except Exception as e:
        logger.error(f"Error in check_and_process_folders: {str(e)}")
        raise

def process_pdf_files(**context):
    """
    Process PDF files for each UUID directory
    """
    pdf_files_by_uuid = context['ti'].xcom_pull(task_ids='check_pdf_folder')
    
    if not pdf_files_by_uuid:
        logger.info("No PDF files to process")
        return
    
    # Trigger dag for each PDF file found
    for uuid, files in pdf_files_by_uuid.items():
        for pdf_file in files:
            context['ti'].xcom_push(key='uuid', value=uuid)
            context['ti'].xcom_push(key='pdf_file', value=pdf_file)
            yield uuid

def branch_func(**context):
    """
    Determine whether to trigger processing or mark no files found
    """
    pdf_files_by_uuid = context['ti'].xcom_pull(task_ids='check_pdf_folder')
    
    if pdf_files_by_uuid:
        return 'trigger_pdf_processing'
    return 'no_files_found'

# DAG definition
with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for PDF files and triggers processing',
    schedule_interval='* * * * *',  # Run every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"]
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    check_folder = PythonOperator(
        task_id='check_pdf_folder',
        python_callable=check_and_process_folders,
        provide_context=True,
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
        provide_context=True,
    )

    no_files = DummyOperator(
        task_id='no_files_found'
    )

    # Use PythonOperator to handle multiple file processing
    process_files = PythonOperator(
        task_id='process_pdf_files',
        python_callable=process_pdf_files,
        provide_context=True,
    )

    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        conf={
            "uuid": "{{ ti.xcom_pull(task_ids='process_pdf_files', key='uuid') }}",
            "pdf_file": "{{ ti.xcom_pull(task_ids='process_pdf_files', key='pdf_file') }}"
        },
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=10,
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )

    # Updated task dependencies
    start >> check_folder >> branch_task
    branch_task >> [no_files, process_files]
    process_files >> trigger_processing >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")