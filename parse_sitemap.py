from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import logging
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_func

# Parent DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'parse_lowtouch_sitemap',
    default_args=default_args,
    description='Parse lowtouch.ai sitemap and trigger HTML processing',
    schedule_interval='0 23 * * *',  # Daily at 11 PM UTC
    start_date=datetime(2025, 4, 16),
    catchup=False,
) as parent_dag:

    def parse_sitemap():
        sitemap_url = 'https://www.lowtouch.ai/sitemap_index.xml'
        uuid = 'febe553a-e665-4000-9cf4-b6ab84b0560f'
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        try:
            # Fetch parent sitemap
            response = requests.get(sitemap_url, headers=headers)
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
            return {'urls': all_urls, 'uuid': uuid}
        
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
        
        for i, url in enumerate(urls):
            # Generate unique run_id for each child DAG run
            child_run_id = f"triggered__{parent_run_id}_{i}"
            logging.info(f"Triggering process_lowtouch_html for URL: {url} with run_id: {child_run_id}")
            
            # Trigger the child DAG
            trigger_dag_func(
                dag_id='process_lowtouch_html',
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
    'process_lowtouch_html',
    default_args=default_args,
    description='Process individual lowtouch.ai HTML page',
    schedule_interval=None,  # Triggered by parent DAG
    start_date=datetime(2025, 4, 16),
    catchup=False,
    max_active_runs=10,  # Limit concurrent runs
    concurrency=10,      # Limit concurrent tasks
) as child_dag:

    def upload_html_to_agentvector(**context):
        try:
            conf = context['dag_run'].conf
            url = conf.get('url')
            uuid = conf.get('uuid')
            
            if not url or not uuid:
                raise ValueError("Missing 'url' or 'uuid' in DAG run configuration")
                
            agentvector_url = f'http://connector:8000/vector/html/{uuid}/{url}'
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            # Call agentvector API to process HTML
            response = requests.post(agentvector_url, headers=headers)
            response.raise_for_status()
            logging.info(f"Successfully uploaded {url} to agentvector. Response: {response.text}")
        
        except Exception as e:
            logging.error(f"Failed to process HTML {url}: {e}")
            raise

    upload_task = PythonOperator(
        task_id='upload_html',
        python_callable=upload_html_to_agentvector,
        provide_context=True,
    )

    upload_task