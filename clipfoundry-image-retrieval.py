from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import logging

# Configuration
default_args = {
    "owner": "lowtouch.ai",
    "depends_on_past": False,
    "retries": 0,
}

def verify_and_process_image(**context):
    conf = context['dag_run'].conf or {}
    
    # 1. defined path or default
    image_path = conf.get("image_path")
    
    logging.info(f"Checking path: {image_path}")

    # 2. Verify file existence (Standard checks)
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Still cannot find file at: {image_path}")
    
    file_size = os.path.getsize(image_path)
    logging.info(f"SUCCESS: Image found! Size: {file_size} bytes.")
    
    return f"Verified image at {image_path}"

with DAG(
    dag_id="image_processor_v1",
    default_args=default_args,
    description="Verifies access to an image path passed via API trigger",
    schedule=None, # Triggered externally only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lowtouch", "image-processing", "tool-test"],
) as dag:

    process_task = PythonOperator(
        task_id="verify_image_access",
        python_callable=verify_and_process_image,
        provide_context=True,
    )

    process_task