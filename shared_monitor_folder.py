from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import logging
import re

# Set up logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# UUID regex pattern
UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)
def check_and_process_folder(**context):
    folder_path = '/appz/data/vector_watch_file_pdf'
    
    try:
        if not os.path.exists(folder_path):
            logger.error(f"Base folder {folder_path} does not exist")
            raise Exception(f"Folder {folder_path} not found")
        
        # Get triggered file path from sensor via XCom
        triggered_path = context['ti'].xcom_pull(task_ids='monitor_folder_changes')
        
        # Extract UUID from path
        uuid_dir = None
        path_parts = triggered_path.split(os.sep)
        for part in path_parts:
            if UUID_PATTERN.match(part):
                uuid_dir = part
                break
        
        if not uuid_dir:
            logger.info("No valid UUID directory found in triggered path")
            return None

        uuid_path = os.path.join(folder_path, uuid_dir)
        pdf_files = []
        
        # Walk through UUID directory, excluding archive
        for root, dirs, files in os.walk(uuid_path):
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
            logger.info(f"Found {len(pdf_files)} PDF files in UUID {uuid_dir}")
            context['ti'].xcom_push(key='uuid', value=uuid_dir)
            return uuid_dir
        
        logger.info(f"No PDF files found in UUID directory {uuid_dir}")
        return None

    except Exception as e:
        logger.error(f"Error in check_and_process_folder: {str(e)}")
        raise

def branch_func(**context):
    uuid = context['ti'].xcom_pull(task_ids='check_pdf_folder', key='uuid')
    if uuid:
        return 'trigger_pdf_processing'
    return 'no_files_found'

# DAG definition
with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors vector folder and UUID subfolders for PDF files',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    monitor_folder = FileSensor(
        task_id='monitor_folder_changes',
        filepath='/appz/data/vector_watch_file_pdf',  # Monitor entire folder
        recursive=True,
        poke_interval=60,
        timeout=3600,
        mode='poke',
        soft_fail=False,
    )

    check_folder = PythonOperator(
        task_id='check_pdf_folder',
        python_callable=check_and_process_folder,
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

    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        conf={"uuid": "{{ ti.xcom_pull(task_ids='check_pdf_folder', key='uuid') }}"},
        reset_dag_run=True,
        wait_for_completion=False,
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )

    # Task dependencies
    start >> monitor_folder >> check_folder >> branch_task
    branch_task >> [no_files, trigger_processing] >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")