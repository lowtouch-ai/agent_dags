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

def fetch_and_inspect_image(**context):
    """
    Step 1: Check if the image exists and calculate size.
    Pushes status to XCom for the next step.
    """
    logging.info("--- STEP 1: Fetching Image ---")
    conf = context['dag_run'].conf or {}
    image_path = conf.get("image_path")
    
    logging.info(f"Looking for file at: {image_path}")

    # Verify file existence
    if not image_path or not os.path.exists(image_path):
        error_result = {
            "status": "error", 
            "message": f"File not found or no path provided: {image_path}",
            "data": None
        }
        logging.error(f"Failed: {error_result['message']}")
        return error_result
    
    # Get metadata
    file_size = os.path.getsize(image_path)
    logging.info(f"Image found. Size: {file_size} bytes.")
    
    # Return valid data for the next step
    return {
        "status": "success",
        "image_path": image_path,
        "file_size": file_size
    }

def generate_final_report(**context):
    """
    Step 2: Consume the inspection data and generate the final Agent response.
    """
    logging.info("--- STEP 2: Generating Final Report ---")
    
    # Pull the return value from the previous task using XCom
    ti = context['ti']
    upstream_data = ti.xcom_pull(task_ids='fetch_and_inspect_image')
    
    if not upstream_data:
        return {"status": "error", "message": "Critical: No data received from upstream task."}

    # Pass through errors if Step 1 failed
    if upstream_data.get("status") == "error":
        logging.warning("Upstream task failed logic check. Passing error to Agent.")
        return upstream_data

    # Construct the final success response
    # This specific dictionary structure is what your Agent parses in ChatDAG.py
    final_response = {
        "status": "success",
        "message": "Image verified and processed successfully",
        "image_path": upstream_data['image_path'],
        "file_size": upstream_data['file_size'],
        "processed_at": datetime.now().isoformat()
    }
    
    logging.info(f"Final Output Prepared: {final_response}")
    return final_response

with DAG(
    dag_id="image_processor_v1",
    default_args=default_args,
    description="Verifies access to an image path passed via API trigger",
    schedule=None, # Triggered externally only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lowtouch", "image-processing", "tool-test"],
) as dag:

    step_1 = PythonOperator(
        task_id="fetch_and_inspect_image",
        python_callable=fetch_and_inspect_image,
        provide_context=True,
    )

    step_2 = PythonOperator(
        task_id="generate_final_report",
        python_callable=generate_final_report,
        provide_context=True,
    )

    # Define Dependency: Step 1 must finish before Step 2 starts
    step_1 >> step_2