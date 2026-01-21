from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import logging
import json

SITEMAP_URL_KEY = "lowtouch_sitemap_url"
UUID_KEY = "lowtouch_uuid"
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
SERVER_NAME = Variable.get("SERVER", default_var="UNKNOWN")

# Slack alert function
def slack_alert(context):
    webhook_url = SLACK_WEBHOOK_URL
    if not webhook_url:
        logging.error("Slack webhook URL not found")
        return

    # In Airflow 3, use context.get() safely
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    run_id = context.get("run_id")

    ti = context.get("task_instance")
    failed_url = ti.xcom_pull(task_ids=task_id, key="failed_url")
    error_message = ti.xcom_pull(task_ids=task_id, key="error_message")

    extra_info = f"\n*Failed URL:* {failed_url}\n*Error:* {error_message}" if failed_url else ""

    message = {
        "text": (
            f":x:*Airflow Task Failed in Server* {SERVER_NAME}\n"
            f"*DAG:* {dag_id}\n"
            f"*Task:* {task_id}\n"
            f"*Run ID:* {run_id}"
            f"{extra_info}"
        )
    }

    try:
        requests.post(
            webhook_url,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        logging.info("Slack alert sent")
    except Exception as e:
        logging.error(f"Failed to send Slack alert: {e}")

# --- Parent DAG ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_alert,
}

with DAG(
    'lowtouch_sitemap_parser',
    default_args=default_args,
    description='Parse lowtouch.ai sitemap and trigger HTML processing',
    schedule='0 23 * * *',
    start_date=datetime(2025, 4, 16),
    catchup=False,
    tags=['lowtouch', 'sitemap','agentvector','parse'],
) as parent_dag:

    def parse_sitemap():
        # Fetch variables INSIDE the task to avoid DB access errors
        sitemap_url = Variable.get(SITEMAP_URL_KEY, default_var="https://www.lowtouch.ai/sitemap_index.xml")
        uuid = Variable.get(UUID_KEY)
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        trigger_configs = []

        try:
            # 1. Fetch Parent Sitemap
            response = requests.get(sitemap_url, headers=headers)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            # 2. Loop through child sitemaps
            for sitemap in root.findall('sitemap:sitemap', ns):
                child_url = sitemap.find('sitemap:loc', ns).text
                logging.info(f"Processing child sitemap: {child_url}")
                
                child_resp = requests.get(child_url, headers=headers)
                if child_resp.status_code != 200: 
                    continue
                    
                child_root = ET.fromstring(child_resp.content)
                
                # 3. Build a list of configs (Dicts) instead of triggering immediately
                for url in child_root.findall('sitemap:url', ns):
                    loc = url.find('sitemap:loc', ns).text
                    if loc.endswith('.html') or loc.endswith('/'):
                        trigger_configs.append({'url': loc, 'uuid': uuid})
            
            logging.info(f"Generated {len(trigger_configs)} configurations for dynamic mapping.")
            return trigger_configs # This list is passed to the next task
        
        except Exception as e:
            logging.error(f"Sitemap parsing failed: {e}")
            raise

    # Step 1: Parse sitemap and return list
    parse_task = PythonOperator(
        task_id='parse_sitemap',
        python_callable=parse_sitemap,
    )

    # Step 2: Dynamically Trigger DAGs (Airflow 3 Native)
    # This operator runs on the Scheduler, so it is allowed to trigger DAGs.
    trigger_task = TriggerDagRunOperator.partial(
        task_id='trigger_child_dags',
        trigger_dag_id='lowtouch_html_to_vector',
        reset_dag_run=True, # Allows re-running for the same execution date
        wait_for_completion=False,
        poke_interval=30
    ).expand(
        conf=parse_task.output # Maps over the list returned by parse_task
    )

# --- Child DAG ---
with DAG(
    'lowtouch_html_to_vector',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 4, 16),
    catchup=False,
    max_active_runs=10,
    max_active_tasks=10, # FIX: Replaced 'concurrency'
    tags=['lowtouch', 'agentvector'],
) as child_dag:

    def upload_html_to_agentvector(**context):
        try:
            # Pull config from the triggered run
            conf = context['dag_run'].conf
            url = conf.get('url')
            uuid = conf.get('uuid')
            
            if not url:
                raise ValueError("No URL provided in DAG run configuration")

            logging.info(f"Processing URL: {url}")
            
            agentvector_url = f'http://vector:8000/vector/html/{uuid}/{url}'
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            response = requests.post(agentvector_url, headers=headers)
            response.raise_for_status()
            logging.info(f"Success: {response.text}")
        
        except Exception as e:
            logging.error(f"Failed to process {url}: {e}")
            
            # Push to XCom for Slack Alert
            ti = context["ti"]
            ti.xcom_push(key="failed_url", value=url)
            ti.xcom_push(key="error_message", value=str(e))
            raise

    upload_task = PythonOperator(
        task_id='upload_html',
        python_callable=upload_html_to_agentvector,
    )