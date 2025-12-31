from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import logging
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_func
import json

# Define Airflow Variables with default values
SITEMAP_URL = Variable.get("lowtouch_sitemap_url", default_var="https://www.lowtouch.ai/sitemap_index.xml")
UUID = Variable.get("lowtouch_uuid")
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
SERVER_NAME = Variable.get("SERVER", default_var="UNKNOWN")

# Slack alert function
def slack_alert(context):
    webhook_url = SLACK_WEBHOOK_URL
    if not webhook_url:
        logging.error("Slack webhook URL not found in Airflow Variables")
        return

    dag_id = context.get("dag_run").dag_id
    task_id = context.get("task_instance").task_id
    run_id = context.get("dag_run").run_id

    # Pull extra info from XCom
    ti = context.get("task_instance")
    failed_url = ti.xcom_pull(task_ids=task_id, key="failed_url")
    error_message = ti.xcom_pull(task_ids=task_id, key="error_message")

    if failed_url and error_message:
        extra_info = f"\n*Failed URL:* {failed_url}\n*Error:* {error_message}"
    else:
        extra_info = ""

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


# Parent DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_alert,  # Added Slack alert
}

with DAG(
    'lowtouch_sitemap_parser',
    default_args=default_args,
    description='Parse lowtouch.ai sitemap and trigger HTML processing',
    schedule_interval='0 23 * * *',  # Daily at 11 PM UTC
    start_date=datetime(2025, 4, 16),
    catchup=False,
    tags=['lowtouch', 'sitemap','agentvector','parse'],
) as parent_dag:

    def parse_sitemap():
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        try:
            # Fetch parent sitemap
            response = requests.get(SITEMAP_URL, headers=headers)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            
            # Namespace for sitemap XML
            ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            # Collect all URLs from child sitemaps
            all_urls = []
            for sitemap in root.findall('sitemap:sitemap', ns):
                child_sitemap_url = sitemap.find('sitemap:loc', ns).text
                logging.info(f"Processing child sitemap: {child_sitemap_url}")
                
                # Fetch child sitemap
                child_response = requests.get(child_sitemap_url, headers=headers)
                child_response.raise_for_status()
                child_root = ET.fromstring(child_response.content)
                
                # Extract URLs from child sitemap
                for url in child_root.findall('sitemap:url', ns):
                    loc = url.find('sitemap:loc', ns).text
                    if loc.endswith('.html') or loc.endswith('/'):
                        all_urls.append(loc)
            
            logging.info(f"Found {len(all_urls)} URLs to process")
            return {'urls': all_urls, 'uuid': UUID}
        
        except Exception as e:
            logging.error(f"Failed to parse sitemap: {e}")
            raise

    parse_task = PythonOperator(
        task_id='parse_sitemap',
        python_callable=parse_sitemap,
    )

    def trigger_child_dags(ti):
        data = ti.xcom_pull(task_ids='parse_sitemap')
        urls = data['urls']
        uuid = data['uuid']
        parent_run_id = ti.dag_run.run_id
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        for i, url in enumerate(urls):
            try:
                # Check if URL is valid (status 200)
                head_response = requests.head(url, headers=headers, allow_redirects=True)
                if head_response.status_code != 200:
                    logging.warning(f"Skipping invalid URL: {url} (status: {head_response.status_code})")
                    continue
            except Exception as e:
                logging.error(f"Failed to check URL {url}: {e}")
                continue
            
            # Generate unique run_id for each child DAG run
            child_run_id = f"triggered__{parent_run_id}_{i}"
            logging.info(f"Triggering lowtouch_html_to_vector for URL: {url} with run_id: {child_run_id}")
            
            # Trigger the child DAG
            trigger_dag_func(
                dag_id='lowtouch_html_to_vector',
                run_id=child_run_id,
                conf={'url': url, 'uuid': uuid},
                replace_microseconds=False,
            )

    trigger_task = PythonOperator(
        task_id='trigger_child_dags',
        python_callable=trigger_child_dags,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    parse_task >> trigger_task

# Child DAG
with DAG(
    'lowtouch_html_to_vector',
    default_args=default_args,
    description='Process individual lowtouch.ai HTML page',
    schedule_interval=None,  # Triggered by parent DAG
    start_date=datetime(2025, 4, 16),
    catchup=False,
    max_active_runs=10,  # Limit concurrent runs
    concurrency=10,      # Limit concurrent tasks
    tags=['lowtouch', 'agentvector', 'load', 'html'],
) as child_dag:

    def upload_html_to_agentvector(**context):
        try:
            conf = context['dag_run'].conf
            url = conf.get('url')
            uuid = conf.get('uuid')
            
            if not url or not uuid:
                raise ValueError("Missing 'url' or 'uuid' in DAG run configuration")
                
            agentvector_url = f'http://vector:8000/vector/html/{uuid}/{url}'
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            # Call agentvector API to process HTML
            response = requests.post(agentvector_url, headers=headers)
            response.raise_for_status()
            logging.info(f"Successfully uploaded {url} to agentvector. Response: {response.text}")
        
        except Exception as e:
            logging.error(f"Failed to process HTML {url}: {e}")

            # Push extra info into XCom for Slack
            ti = context["ti"]
            ti.xcom_push(key="failed_url", value=url)
            ti.xcom_push(key="error_message", value=str(e))

            raise

    upload_task = PythonOperator(
        task_id='upload_html',
        python_callable=upload_html_to_agentvector,
        provide_context=True,
    )

    upload_task
