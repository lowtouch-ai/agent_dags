from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import os
import requests
from pathlib import Path
import logging
import shutil

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='shared_process_file_pdf2vector',
    default_args=default_args,
    description='Process a single PDF file to vector API and archive it',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={'uuid': None, 'file_path': None}
)
def process_pdf_dag():
    @task
    def process_file(uuid, file_path):
        """Upload a single PDF file to the API and archive it."""
        if not uuid or not file_path:
            logger.error("UUID or file_path not provided - stopping DAG")
            return

        base_api_endpoint = "http://vector:8000/vector/pdf/"
        pdf_file = os.path.basename(file_path)
        api_endpoint = f"{base_api_endpoint}{uuid}/{pdf_file}"
        target_path = f"/appz/data/vector_watch_file_pdf/{uuid}"

        if not os.path.exists(file_path):
            logger.error(f"File {file_path} not found - stopping DAG")
            return

        # Extract tags from path
        path_parts = Path(file_path).relative_to(target_path).parts
        tags = list(path_parts[:-1]) if len(path_parts) > 1 else []

        # Prepare upload data
        files = {'file': (pdf_file, open(file_path, 'rb'), 'application/pdf')}
        params = {'tags': ','.join(tags)} if tags else {}

        try:
            response = requests.post(api_endpoint, files=files, params=params)
            response.raise_for_status()
            logger.info(f"Uploaded {pdf_file} to {api_endpoint} with tags: {tags}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error uploading {pdf_file}: {str(e)}")
        finally:
            files['file'][1].close()
            # Archive the file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_path = os.path.join(target_path, 'archive', timestamp)
            os.makedirs(archive_path, exist_ok=True)
            archive_destination = os.path.join(archive_path, pdf_file)
            try:
                shutil.move(file_path, archive_destination)
                logger.info(f"Archived {pdf_file} to {archive_destination}")
            except Exception as e:
                logger.error(f"Error archiving {pdf_file}: {str(e)}")

    @task
    def extract_params(**kwargs):
        """Extract UUID and file_path from dag_run.conf."""
        conf = kwargs['dag_run'].conf
        return {'uuid': conf.get('uuid'), 'file_path': conf.get('file_path')}

    params = extract_params()
    process_file(params['uuid'], params['file_path'])

dag = process_pdf_dag()