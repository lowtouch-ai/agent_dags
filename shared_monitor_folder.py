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

# UUID regex pattern
UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

def check_and_process_folder(**context):
    folder_path = '/appz/data/vector_watch_file_pdf'
    pdf_files_found = []
    
    try:
        if not os.path.exists(folder_path):
            logger.error(f"Base folder {folder_path} does not exist")
            raise Exception(f"Folder {folder_path} not found")
        
        # Find all UUID directories
        for dir_name in os.listdir(folder_path):
            if not UUID_PATTERN.match(dir_name):
                continue
                
            uuid_path = os.path.join(folder_path, dir_name)
            if not os.path.isdir(uuid_path):
                continue
                
            # Walk through UUID directory, excluding archive
            for root, dirs, files in os.walk(uuid_path):
                if 'archive' in dirs:
                    dirs.remove('archive')
                if 'archive' in root.lower():
                    continue
                    
                # Collect PDF files with their full paths and UUID
                for file in files:
                    if file.lower().endswith('.pdf'):
                        full_path = os.path.join(root, file)
                        pdf_files_found.append({
                            'uuid': dir_name,
                            'file_path': full_path,
                            'file_name': file
                        })
        
        if not pdf_files_found:
            logger.info("No PDF files found in any UUID directories")
            return 'no_files_found'
            
        # Push list of found files to XCom
        context['ti'].xcom_push(key='pdf_files', value=pdf_files_found)
        logger.info(f"Found {len(pdf_files_found)} PDF files across UUID directories")
        return 'process_files'
        
    except Exception as e:
        logger.error(f"Error in check_and_process_folder: {str(e)}")
        raise

def branch_func(**context):
    return context['ti'].xcom_pull(task_ids='check_pdf_folder')

def create_trigger_task(pdf_file_info, idx):
    return TriggerDagRunOperator(
        task_id=f'trigger_pdf_processing_{idx}',
        trigger_dag_id='shared_process_file_pdf2vector',
        conf={
            'uuid': pdf_file_info['uuid'],
            'file_path': pdf_file_info['file_path'],
            'file_name': pdf_file_info['file_name']
        },
        reset_dag_run=True,
        wait_for_completion=True,
    )

# DAG definition
with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for PDF files and triggers processing for each file',
    schedule_interval='* * * * *',  # Runs every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"],
    max_active_runs=1  # Prevents overlapping runs
) as dag:

    start = DummyOperator(task_id='start')

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

    no_files = DummyOperator(task_id='no_files_found')

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )

    # Dynamic task creation will happen at runtime
    def create_dynamic_tasks(**context):
        pdf_files = context['ti'].xcom_pull(task_ids='check_pdf_folder', key='pdf_files')
        if not pdf_files:
            return
        
        trigger_tasks = [
            create_trigger_task(pdf_file, idx)
            for idx, pdf_file in enumerate(pdf_files)
        ]
        
        # Set dependencies for trigger tasks
        for trigger_task in trigger_tasks:
            branch_task >> trigger_task >> end

    # Set initial dependencies
    start >> check_folder >> branch_task
    branch_task >> no_files >> end

    # Add dynamic task creation
    from airflow.operators.python import PythonOperator
    dynamic_task_creation = PythonOperator(
        task_id='create_dynamic_tasks',
        python_callable=create_dynamic_tasks,
        provide_context=True,
        trigger_rule='all_success'
    )
    branch_task >> dynamic_task_creation >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")