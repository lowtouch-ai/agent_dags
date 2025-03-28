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
def prepare_file_configs(file_path):
    """Prepare configuration for each detected file"""
    if not file_path:
        return []
    
    dir_name = os.path.basename(os.path.dirname(file_path))
    if not UUID_PATTERN.match(dir_name):
        return []
        
    return [{
        'uuid': dir_name,
        'file_path': file_path
    }]

with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for PDF files and triggers processing',
    schedule_interval='* * * * *',  # Runs every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"],
    max_active_runs=1,
    concurrency=50,
    max_active_tasks=50
) as dag:

    start = DummyOperator(task_id='start')
    
    # Sensor to monitor the folder
    monitor_folder = FileSensor(
        task_id='monitor_pdf_folder',
        filepath='/appz/data/vector_watch_file_pdf/*/*.pdf',  # Wildcard pattern for UUID folders
        recursive=True,
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # Timeout after 1 hour
        mode='reschedule',  # More efficient than poke
        fs_conn_id='fs_default' 
    )
    
    # Process the found file
    prepare_config = prepare_file_configs(
        task_id='prepare_file_config',
        file_path="{{ task_instance.xcom_pull(task_ids='monitor_pdf_folder', key='file_path') }}"
    )
    
    # Trigger the processing DAG
    trigger_processing = TriggerDagRunOperator.partial(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=10,
        execution_timeout=timedelta(minutes=30),
    ).expand(
        conf=prepare_config
    )
    
    end = DummyOperator(task_id='end', trigger_rule='all_done')
    
    # DAG flow
    start >> monitor_folder >> prepare_config >> trigger_processing >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")