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

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'retries': 1,
    "retry_delay": timedelta(seconds=15),
}

UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

@task
def process_found_file(file_path):
    """Process the file path and return config for trigger"""
    folder_path = '/appz/data/vector_watch_file_pdf'
    relative_path = file_path.replace(folder_path + '/', '')
    uuid = relative_path.split('/')[0]
    
    return {
        'uuid': uuid,
        'file_path': file_path
    }

with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for new PDF files and triggers processing',
    schedule_interval=None,  # We'll use sensor instead of schedule
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"],
    max_active_runs=1,
    concurrency=50,
) as dag:

    start = DummyOperator(task_id='start')

    # FileSensor to monitor for new PDF files
    monitor_folder = FileSensor(
        task_id='monitor_new_pdfs',
        filepath='/appz/data/vector_watch_file_pdf/*/*.pdf',  # Pattern to match UUID subfolders
        recursive=True,
        poke_interval=60,  # Check every minute
        timeout=24*60*60,  # Timeout after 24 hours
        mode='poke',  # Keep checking until file appears
        soft_fail=False,  # Fail if timeout reached
        filter_by_pattern=True,  # Use the pattern matching
        fs_conn_id='fs_default'  # Default filesystem connection
    )

    # Process the found file
    process_file = process_found_file(
        monitor_folder.output
    )

    # Trigger the processing DAG
    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        conf=process_file,
        wait_for_completion=False,
        reset_dag_run=True,
        execution_timeout=timedelta(minutes=30),
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )

    # Dependencies
    start >> monitor_folder >> process_file >> trigger_processing >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")