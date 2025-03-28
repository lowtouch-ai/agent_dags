from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import os
import re
import logging

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'retries': 1,
}

UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

@dag(
    dag_id='shared_monitor_folder_pdf',
    default_args=default_args,
    description='Monitors UUID folders for PDF files and triggers processing per file',
    schedule_interval='* * * * *',  # Every minute polling
    start_date=days_ago(1),
    catchup=False,
)
def monitor_folder_dag():
    @task
    def check_pdf_folder():
        """Scan for all PDF files across UUID directories, excluding 'archive'."""
        folder_path = '/appz/data/vector_watch_file_pdf'
        if not os.path.exists(folder_path):
            logger.error(f"Base folder {folder_path} does not exist")
            raise Exception(f"Folder {folder_path} not found")

        pdf_files_info = []  # List of dicts: {'uuid': uuid, 'file_path': path}
        for dir_name in os.listdir(folder_path):
            if UUID_PATTERN.match(dir_name):
                full_path = os.path.join(folder_path, dir_name)
                if os.path.isdir(full_path):
                    for root, dirs, files in os.walk(full_path):
                        if 'archive' in dirs:
                            dirs.remove('archive')
                        if 'archive' in root.lower():
                            continue
                        for f in files:
                            if f.lower().endswith('.pdf'):
                                pdf_files_info.append({
                                    'uuid': dir_name,
                                    'file_path': os.path.join(root, f)
                                })
        if pdf_files_info:
            logger.info(f"Found {len(pdf_files_info)} PDF files across UUID directories")
        else:
            logger.info("No PDF files found in any UUID directories")
        return pdf_files_info

    @task.branch
    def branch_func(pdf_files_info):
        """Branch based on whether PDF files are found."""
        if pdf_files_info:
            return 'trigger_pdf_processing'
        return 'no_files_found'

    @task
    def no_files_found():
        logger.info("No PDF files found in any UUID directories - stopping DAG")

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED)

    pdf_files_info = check_pdf_folder()
    branch = branch_func(pdf_files_info)

    trigger_tasks = TriggerDagRunOperator.partial(
        task_id='trigger_pdf_processing',
        trigger_dag_id='shared_process_file_pdf2vector',
        reset_dag_run=True,
        wait_for_completion=False,  # Enable parallel execution
    ).expand(
        conf=[{'uuid': info['uuid'], 'file_path': info['file_path']} for info in pdf_files_info]
    )

    no_files = no_files_found()

    # Dependencies
    start >> pdf_files_info >> branch
    branch >> trigger_tasks >> end
    branch >> no_files >> end

dag = monitor_folder_dag()