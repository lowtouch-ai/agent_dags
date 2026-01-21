from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import os
import json
import git  # Requires gitpython
from github import Github  # Requires pygithub
from urllib.parse import urlparse
from filelock import FileLock  # Requires filelock package

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "lowtouch_ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Processed files tracking file
PROCESSED_FILES_PATH = Variable.get("DSX_PROCESSED_FILES_PATH", "/tmp/processed_dsx_files.json")
IN_PROGRESS_FILES_PATH = Variable.get("DSX_IN_PROGRESS_FILES_PATH", "/tmp/in_progress_dsx_files.json")


def load_in_progress_files():
    """Load the list of in-progress DSX files."""
    try:
        if os.path.exists(IN_PROGRESS_FILES_PATH):
            with open(IN_PROGRESS_FILES_PATH, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        return set()
    except Exception as e:
        logging.error(f"Error loading in-progress files: {str(e)}")
        return set()

def save_in_progress_files(in_progress_files):
    """Save the list of in-progress DSX files."""
    try:
        with open(IN_PROGRESS_FILES_PATH, 'w', encoding='utf-8') as f:
            json.dump(list(in_progress_files), f)
    except Exception as e:
        logging.error(f"Error saving in-progress files: {str(e)}")
        raise


def load_processed_files():
    """Load the list of processed DSX files."""
    try:
        if os.path.exists(PROCESSED_FILES_PATH):
            with open(PROCESSED_FILES_PATH, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        return set()
    except Exception as e:
        logging.error(f"Error loading processed files: {str(e)}")
        return set()

def save_processed_files(processed_files):
    """Save the list of processed DSX files."""
    try:
        with open(PROCESSED_FILES_PATH, 'w', encoding='utf-8') as f:
            json.dump(list(processed_files), f)
    except Exception as e:
        logging.error(f"Error saving processed files: {str(e)}")
        raise

def list_unprocessed_dsx_files(ti, **context):
    try:
        git_url = Variable.get("DSX_GIT_URL", "https://github.com/lowtouch-ai/datastage-jobs.git")
        source_branch = Variable.get("DSX_SOURCE_BRANCH", "main")  # New variable for source branch
        poc_dir = "jobs/POC"
        
        # Parse repo name from git_url
        parsed = urlparse(git_url)
        repo_name = parsed.path.strip('/').replace('.git', '')
        
        gh_token = Variable.get("GITHUB_TOKEN")
        g = Github(gh_token)
        gh_repo = g.get_repo(repo_name)
        
        contents = gh_repo.get_contents(poc_dir, ref=source_branch)  # Specify branch
        dsx_files = [c.name for c in contents if c.name.endswith('.dsx')]
        logging.info(f"Found {len(dsx_files)} DSX files in {poc_dir} on branch {source_branch}")
        
        if not dsx_files:
            raise ValueError(f"No .dsx files found in {poc_dir}")
        
        # Load processed files
        processed_files = load_processed_files()
        in_progress_files = load_in_progress_files()
        # Filter unprocessed files
        unprocessed_files = [f for f in dsx_files if f not in processed_files and f not in in_progress_files]
        
        if not unprocessed_files:
            logging.info("No new DSX files to process")
            ti.xcom_push(key="unprocessed_files", value=[])
        else:
            logging.info(f"Found {len(unprocessed_files)} unprocessed DSX files: {unprocessed_files}")
            ti.xcom_push(key="unprocessed_files", value=unprocessed_files)
        
        ti.xcom_push(key="repo_name", value=repo_name)
        ti.xcom_push(key="poc_dir", value=poc_dir)
        ti.xcom_push(key="git_url", value=git_url)
    except Exception as e:
        logging.error(f"Error listing DSX files: {str(e)}")
        raise

def branch_function(**kwargs):
    """Decide which task to execute based on the presence of unprocessed files."""
    ti = kwargs['ti']
    unprocessed_files = ti.xcom_pull(task_ids="list_unprocessed_dsx_files", key="unprocessed_files")
    if unprocessed_files and len(unprocessed_files) > 0:
        logging.info("Unprocessed files found, proceeding to trigger transform tasks.")
        return "trigger_transform_tasks"
    else:
        logging.info("No unprocessed files found, proceeding to no_files_task.")
        return "no_files_task"

def trigger_transform_tasks(**kwargs):
    """Trigger 'netexa_dsx_oracle_process_file' for each unprocessed file."""
    ti = kwargs['ti']
    unprocessed_files = ti.xcom_pull(task_ids="list_unprocessed_dsx_files", key="unprocessed_files")
    logging.info(f"Retrieved {len(unprocessed_files) if unprocessed_files else 0} unprocessed files from XCom.")
    if not unprocessed_files:
        logging.info("No unprocessed files found in XCom, skipping trigger.")
        return
    
    in_progress_files = load_in_progress_files()
    
    for file in unprocessed_files:
        
        in_progress_files.add(file)
        save_in_progress_files(in_progress_files)
        
        task_id = f"trigger_transform_{file.replace('.', '_').replace('-', '_')}"
        logging.info(f"Triggering Transform DAG for file: {file}")
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="netexa_dsx_oracle_process_file",
            conf={"dsx_filename": file},
        )
        trigger_task.execute(context=kwargs)

def no_files_found(**kwargs):
    """Log when no files are found."""
    logging.info("No new DSX files found to process.")

with DAG(
    "netexa_dsx_oracle_monitor_github",
    default_args=default_args,
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=["dsx", "monitor", "github"]
) as dag:
    
    list_files_task = PythonOperator(
        task_id="list_unprocessed_dsx_files",
        python_callable=list_unprocessed_dsx_files,
        provide_context=True
    )
    
    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function,
        provide_context=True
    )
    
    trigger_transform_task = PythonOperator(
        task_id="trigger_transform_tasks",
        python_callable=trigger_transform_tasks,
        provide_context=True
    )
    
    no_files_task = PythonOperator(
        task_id="no_files_task",
        python_callable=no_files_found,
        provide_context=True
    )
    
    # Set task dependencies
    list_files_task >> branch_task
    branch_task >> [trigger_transform_task, no_files_task]

