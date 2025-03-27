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
    
    try:
        if not os.path.exists(folder_path):
            logger.error(f"Base folder {folder_path} does not exist")
            raise Exception(f"Folder {folder_path} not found")
        
        pdf_files_info = []
        for dir_name in os.listdir(folder_path):
            if UUID_PATTERN.match(dir_name):
                full_path = os.path.join(folder_path, dir_name)
                if os.path.isdir(full_path):
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
            logger.info("No PDF files found in any UUID directories")
            return 'no_files_found'
            
        # Push list of PDF files info to XCom
        context['ti'].xcom_push(key='pdf_files_info', value=pdf_files_info)
        return 'process_files'
        
    except Exception as e:
        logger.error(f"Error in check_and_process_folder: {str(e)}")
        raise

def create_trigger_tasks(**context):
    pdf_files_info = context['ti'].xcom_pull(task_ids='check_pdf_folder', key='pdf_files_info')
    if not pdf_files_info:
        return 'no_files_found'
        
    # Create dynamic trigger tasks
    trigger_tasks = []
    for idx, file_info in enumerate(pdf_files_info):
        task_id = f'trigger_pdf_processing_{idx}'
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id='shared_process_file_pdf2vector',
            conf={
                'uuid': file_info['uuid'],
                'file_path': file_info['file_path']
            },
            reset_dag_run=True,
            wait_for_completion=True,
            dag=dag
        )
        trigger_tasks.append(task_id)
    
    context['ti'].xcom_push(key='trigger_task_ids', value=trigger_tasks)
    return trigger_tasks

# DAG definition
with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for PDF files and triggers processing per file',
    schedule_interval='* * * * *',  # Runs every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"]
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    check_folder = PythonOperator(
        task_id='check_pdf_folder',
        python_callable=check_and_process_folder,
        provide_context=True,
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=create_trigger_tasks,
        provide_context=True,
    )

    no_files = DummyOperator(
        task_id='no_files_found'
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )

    # Base dependencies
    start >> check_folder >> branch_task
    branch_task >> no_files >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")