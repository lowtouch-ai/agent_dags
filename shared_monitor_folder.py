from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task
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

# UUID regex pattern
UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

# Task to check the folder and return PDF info
@task
def check_pdf_folder():
    folder_path = '/appz/data/vector_watch_file_pdf'
    pdf_files_info = []
    
    try:
        if not os.path.exists(folder_path):
            logger.error(f"Base folder {folder_path} does not exist")
            raise Exception(f"Folder {folder_path} not found")
        
        # Scan for UUID directories
        for dir_name in os.listdir(folder_path):
            if not UUID_PATTERN.match(dir_name):
                continue
                
            full_path = os.path.join(folder_path, dir_name)
            if not os.path.isdir(full_path):
                continue
                
            # Walk through UUID directory, excluding archive
            for root, dirs, files in os.walk(full_path):
                if 'archive' in dirs:
                    dirs.remove('archive')
                if 'archive' in root.lower():
                    continue
                    
                for file in files:
                    if file.lower().endswith('.pdf'):
                        pdf_files_info.append({
                            'uuid': dir_name,
                            'file_path': os.path.join(root, file)
                        })
        
        if not pdf_files_info:
            logger.info("  No PDF files found in any UUID directories")
        
        return pdf_files_info
        
    except Exception as e:
        logger.error(f"Error in check_pdf_folder: {str(e)}")
        raise

# DAG definition
with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for PDF files and triggers processing',
    schedule_interval='* * * * *',  # Runs every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"],
    max_active_runs=1
) as dag:

    # Start task
    start = DummyOperator(task_id='start')
    
    # Check folder task
    check_folder_task = check_pdf_folder()
    
    # Dynamically mapped trigger tasks
    trigger_tasks = TriggerDagRunOperator.partial(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        wait_for_completion=False,
        reset_dag_run=True,
        execution_timeout=timedelta(minutes=30),
    ).expand(
        conf=check_folder_task,
    )
    
    # End task
    end = DummyOperator(task_id='end', trigger_rule='none_failed')
    
    # Set dependencies
    start >> check_folder_task >> trigger_tasks >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")