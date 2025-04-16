from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import logging

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
        
        # Return URLs and UUID for downstream tasks
        return {'urls': all_urls, 'uuid': uuid}

    parse_task = PythonOperator(
        task_id='parse_sitemap',
        python_callable=parse_sitemap,
    )

    # Create trigger tasks statically
    def get_urls(ti):
        data = ti.xcom_pull(task_ids='parse_sitemap')
        return data['urls'], data['uuid']

    # Define a reasonable number of trigger tasks (adjust based on needs)
    MAX_TRIGGERS = 100  # Limit to avoid excessive task creation
    for i in range(MAX_TRIGGERS):
        trigger_task = TriggerDagRunOperator(
            task_id=f'trigger_html_processing_{i}',
            trigger_dag_id='process_lowtouch_html',
            conf={
                'url': "{{ ti.xcom_pull(task_ids='parse_sitemap')['urls'][%d] }}" % i,
                'uuid': "{{ ti.xcom_pull(task_ids='parse_sitemap')['uuid'] }}"
            },
            execution_date='{{ execution_date }}',
            dag=parent_dag,
            do_xcom_push=False,
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
) as child_dag:

    def upload_html_to_agentvector(**context):
        try:
            conf = context['dag_run'].conf
            url = conf.get('url')
            uuid = conf.get('uuid')
            
            if not url or not uuid:
                raise ValueError("Missing 'url' or 'uuid' in DAG run configuration")
                
            agentvector_url = f'http://localhost:8082/vector/html/{uuid}/{url}'
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            # Call agentvector API to process HTML
            response = requests.post(agentvector_url, headers=headers)
            response.raise_for_status()
            logging.info(f"Successfully uploaded {url} to agentvector")
        
        except Exception as e:
            logging.error(f"Failed to process HTML: {e}")
            raise

    upload_task = PythonOperator(
        task_id='upload_html',
        python_callable=upload_html_to_agentvector,
        provide_context=True,
    )

    upload_task