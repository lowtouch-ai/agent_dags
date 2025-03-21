from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# UUID regex pattern (matches standard UUID format: 8-4-4-4-12)
UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

def check_and_process_folder(**context):
    folder_path = '/appz/data/vector_watch_file_pdf'
    
    try:
        # Check if base folder exists
        if not os.path.exists(folder_path):
            logger.error(f"Base folder {folder_path} does not exist")
            raise Exception(f"Folder {folder_path} not found")
        
        # Look for UUID directories using regex
        uuid_dirs = []
        for dir_name in os.listdir(folder_path):
            if UUID_PATTERN.match(dir_name):
                full_path = os.path.join(folder_path, dir_name)
                if os.path.isdir(full_path):
                    uuid_dirs.append(dir_name)
        
        if not uuid_dirs:
            logger.info("No UUID directories found in the monitored folder")
            return None

        # Process each UUID directory
        for uuid_dir in uuid_dirs:
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
                return {'uuid': uuid_dir}
            else:
                logger.info(f"No PDF files found in UUID directory {uuid_dir}")
        
        logger.info("No PDF files found in any UUID directories")
        return None

    except Exception as e:
        logger.error(f"Error in check_and_process_folder: {str(e)}")
        raise

# DAG definition
with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for PDF files and triggers processing',
    schedule_interval='* * * * *',  # Runs every minute
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Start task
    start = DummyOperator(
        task_id='start'
    )

    # Check folder task
    check_folder = PythonOperator(
        task_id='check_pdf_folder',
        python_callable=check_and_process_folder,
        provide_context=True,
    )

    # Dummy operator for no files found
    no_files = DummyOperator(
        task_id='no_files_found'
    )

    # Trigger processing DAG when files are found
    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        conf={"uuid": "{{ task_instance.xcom_pull(task_ids='check_pdf_folder')['uuid'] }}"},
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    # End task
    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed'  # Continue even if one branch fails
    )

    # Task dependencies with branching logic
    start >> check_folder
    check_folder >> [no_files, trigger_processing]  # Branch based on XCom return
    [no_files, trigger_processing] >> end

logger.info("DAG shared_monitor_folder_pdf loaded successfully")