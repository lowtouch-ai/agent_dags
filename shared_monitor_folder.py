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
def extract_file_info(file_path):
    """Extract UUID and file path from the detected file."""
    if not file_path or not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist")
        return None
    
    # Extract UUID from the directory structure
    relative_path = file_path.replace('/appz/data/vector_watch_file_pdf/', '')
    parts = relative_path.split(os.sep)
    uuid = next((part for part in parts if UUID_PATTERN.match(part)), None)
    
    if not uuid:
        logger.error(f"No valid UUID found in path: {file_path}")
        return None
    
    return {'uuid': uuid, 'file_path': file_path}

# DAG definition
with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for new PDF files and triggers processing',
    schedule_interval=None,  # FileSensor will drive execution
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"],
    max_active_runs=1,
    concurrency=50,
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # FileSensor to detect new PDF files
    monitor_folder = FileSensor(
        task_id='monitor_pdf_files',
        filepath='/appz/data/vector_watch_file_pdf/*/*.pdf',  # Adjust pattern as needed
        recursive=True,  # Scan subdirectories
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # Timeout after 1 hour if no new files
        mode='poke',  # Keep checking until a file is found
        filter_by_pattern=True,  # Use the filepath pattern
    )

    # Extract file info from the detected file
    extract_info = extract_file_info(monitor_folder.output)

    # Trigger the processing DAG
    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        conf={'uuid': extract_info['uuid'], 'file_path': extract_info['file_path']},
        wait_for_completion=False,
        reset_dag_run=True,
        execution_timeout=timedelta(minutes=30),
    )

    # End task
    end = DummyOperator(task_id='end', trigger_rule='all_done')

    # Set dependencies
    start >> monitor_folder >> extract_info >> trigger_processing >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")