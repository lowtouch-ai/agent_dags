from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task
import os
import logging
import re
import shutil

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
def check_and_move_pdf_folder():
    base_path = '/appz/data/vector_watch_file_pdf'
    pdf_files_info = []
    
    try:
        if not os.path.exists(base_path):
            logger.error(f"Base folder {base_path} does not exist")
            raise Exception(f"Folder {base_path} not found")
        
        for dir_name in os.listdir(base_path):
            if not UUID_PATTERN.match(dir_name):
                continue
                
            full_path = os.path.join(base_path, dir_name)
            if not os.path.isdir(full_path):
                continue
                
            processing_path = os.path.join(full_path, 'processsing_pdf')
            os.makedirs(processing_path, exist_ok=True)
            
            for root, dirs, files in os.walk(full_path):
                if 'archive' in root.lower() or 'processsing_pdf' in root.lower():
                    continue
                    
                for file in files:
                    if file.lower().endswith('.pdf'):
                        original_path = os.path.join(root, file)
                        new_path = os.path.join(processing_path, file)
                        
                        if not os.path.exists(new_path):
                            shutil.move(original_path, new_path)
                            logger.info(f"Moved {original_path} to {new_path}")
                            pdf_files_info.append({
                                'uuid': dir_name,
                                'file_path': new_path,
                                'tags': os.path.relpath(root, full_path).split(os.sep)
                            })
        
        # Cleanup empty subfolders
        for dir_name in os.listdir(base_path):
            if UUID_PATTERN.match(dir_name):
                full_path = os.path.join(base_path, dir_name)
                for root, dirs, files in os.walk(full_path, topdown=False):
                    if 'archive' not in root.lower() and 'processsing_pdf' not in root.lower():
                        if not os.listdir(root):
                            shutil.rmtree(root)
                            logger.info(f"Removed empty folder: {root}")
        
        if not pdf_files_info:
            logger.info("No PDF files found to process")
            return []  # Return empty list instead of None
        
        logger.info(f"Found {len(pdf_files_info)} PDF files to process")
        return pdf_files_info
        
    except Exception as e:
        logger.error(f"Error in check_and_move_pdf_folder: {str(e)}")
        raise

with DAG(
    'shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for PDF files and triggers processing',
    schedule_interval='* * * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=["shared", "folder", "monitor", "pdf", "rag"],
    max_active_runs=1,
   
) as dag:

    start = DummyOperator(task_id='start')
    check_folder_task = check_and_move_pdf_folder()
    no_files_found = DummyOperator(task_id='no_files_found')
    
    trigger_processing = TriggerDagRunOperator.partial(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=10,
        execution_timeout=timedelta(minutes=30),
    ).expand(
        conf=check_folder_task.map(
            lambda x: {'uuid': x['uuid'], 'file_path': x['file_path'], 'tags': x['tags']}
            if x else {}  # Handle empty case
        )
    )
    
    end = DummyOperator(task_id='end', trigger_rule='all_done')
    
    start >> check_folder_task
    check_folder_task >> [trigger_processing, no_files_found]
    trigger_processing >> end
    no_files_found >> end