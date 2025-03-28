from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.decorators import task
import os
import logging
import re
from pathlib import Path

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

@task
def filter_new_pdf_files(file_path):
    """
    Filter out already processed files and return new ones.
    Uses a simple text file to track processed files (could be replaced with XCom or a database).
    """
    processed_log = '/appz/data/processed_files.log'  # File to track processed files
    new_files_info = []
    
    try:
        # Load previously processed files
        processed_files = set()
        if os.path.exists(processed_log):
            with open(processed_log, 'r') as f:
                processed_files = set(line.strip() for line in f)
        
        # Check if the file is new
        if file_path and file_path.lower().endswith('.pdf') and file_path not in processed_files:
            # Extract UUID from the path
            parts = Path(file_path).parts
            for part in parts:
                if UUID_PATTERN.match(part):
                    uuid = part
                    break
            else:
                logger.warning(f"No UUID found in path: {file_path}")
                return []
                
            new_files_info.append({
                'uuid': uuid,
                'file_path': file_path
            })
            
            # Mark file as processed
            with open(processed_log, 'a') as f:
                f.write(f"{file_path}\n")
        
        return new_files_info if new_files_info else []
        
    except Exception as e:
        logger.error(f"Error in filter_new_pdf_files: {str(e)}")
        raise

# DAG definition
with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for new PDF files and triggers processing',
    schedule_interval='* * * * *',  # Runs every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"],
    max_active_runs=1,
    concurrency=50,
) as dag:

    # Start task
    start = DummyOperator(task_id='start')
    
    # FileSensor to monitor the folder
    monitor_folder = FileSensor(
        task_id='monitor_pdf_folder',
        filepath='/appz/data/vector_watch_file_pdf',  # Base directory
        recursive=True,  # Check subdirectories
        fs_conn_id='fs_default',  # Default filesystem connection
        poke_interval=60,  # Check every 60 seconds (aligned with schedule)
        timeout=300,  # Timeout after 5 minutes if no new files
        mode='poke',  # Keep checking until a file is found
        filter_pattern=r'.*\.pdf$',  # Match PDF files only
        excluded_paths=['*/archive/*'],  # Exclude archive directories
    )
    
    # Filter new files task
    filter_new_files_task = filter_new_pdf_files(monitor_folder.output)
    
    # No files found task
    no_files_found = DummyOperator(
        task_id='no_files_found',
        trigger_rule='one_success'
    )
    
    # Trigger processing for each new PDF file
    trigger_processing = TriggerDagRunOperator.partial(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        wait_for_completion=False,
        reset_dag_run=True,
        execution_timeout=timedelta(minutes=30),
    ).expand(
        conf=filter_new_files_task.map(
            lambda x: {'uuid': x['uuid'], 'file_path': x['file_path']} if x else {}
        )
    )
    
    # End task
    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )
    
    # Set dependencies
    start >> monitor_folder >> filter_new_files_task
    filter_new_files_task >> [trigger_processing, no_files_found]
    trigger_processing >> end
    no_files_found >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")