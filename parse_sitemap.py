from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import logging
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_func
import urllib.parse
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Initialize session with retries and connection pooling
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))

# Airflow Variables
SITEMAP_URL = Variable.get("lowtouch_sitemap_url", default_var="https://www.lowtouch.ai/sitemap_index.xml")
UUID = Variable.get("lowtouch_uuid", default_var="febe553a-e665-4000-9cf4-b6ab84b0560f")
AGENTVECTOR_BASE_URL = Variable.get("agentvector_base_url", default_var="http://vector:8000/vector/html")
USER_AGENT = Variable.get("user_agent", default_var="Mozilla/5.0 (compatible; AirflowBot/1.0)")
CHILD_DAG_CONCURRENCY = int(Variable.get("child_dag_concurrency", default_var=10))
RATE_LIMIT_DELAY = float(Variable.get("rate_limit_delay", default_var=0.5))  # Seconds between requests

logging.info(f"Using sitemap URL: {SITEMAP_URL}, UUID: {UUID}, AgentVector URL: {AGENTVECTOR_BASE_URL}, User-Agent: {USER_AGENT}, Child DAG Concurrency: {CHILD_DAG_CONCURRENCY}, Rate Limit Delay: {RATE_LIMIT_DELAY}")

# Parent DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'shared_parse_website_sitemap',
    default_args=default_args,
    description='Parse lowtouch.ai sitemap and trigger HTML processing',
    schedule_interval='0 23 * * *',  # Daily at 11 PM UTC
    start_date=datetime(2025, 4, 16),
    catchup=False,
) as parent_dag:

    def parse_sitemap():
        headers = {'User-Agent': USER_AGENT}
        all_urls = []
        
        try:
            # Fetch parent sitemap
            logging.info(f"Fetching parent sitemap: {SITEMAP_URL}")
            response = session.get(SITEMAP_URL, headers=headers, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            
            # Use fixed namespace as in original code
            ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            # Process child sitemaps
            child_sitemaps = root.findall('sitemap:sitemap', ns)
            logging.info(f"Found {len(child_sitemaps)} child sitemaps")
            
            for sitemap in child_sitemaps:
                loc_element = sitemap.find('sitemap:loc', ns)
                if loc_element is None or loc_element.text is None:
                    logging.warning(f"Skipping sitemap entry with missing or invalid 'loc' element: {ET.tostring(sitemap, encoding='unicode')}")
                    continue
                child_sitemap_url = loc_element.text
                logging.info(f"Processing child sitemap: {child_sitemap_url}")
                
                try:
                    # Fetch child sitemap with rate limiting
                    time.sleep(RATE_LIMIT_DELAY)
                    child_response = session.get(child_sitemap_url, headers=headers, timeout=10)
                    child_response.raise_for_status()
                    child_root = ET.fromstring(child_response.content)
                    
                    # Extract URLs
                    urls = child_root.findall('sitemap:url', ns)
                    for url in urls:
                        loc = url.find('sitemap:loc', ns)
                        if loc is None or loc.text is None:
                            logging.warning(f"Skipping URL entry with missing or invalid 'loc': {ET.tostring(url, encoding='unicode')}")
                            continue
                        loc_text = loc.text
                        if loc_text.endswith('.html') or loc_text.endswith('/'):
                            all_urls.append(loc_text)
                            logging.debug(f"Added URL: {loc_text}")
                
                except Exception as e:
                    logging.error(f"Failed to process child sitemap {child_sitemap_url}: {e}")
                    continue
            
            logging.info(f"Total URLs found: {len(all_urls)}")
            if not all_urls:
                logging.warning("No valid URLs found in sitemap")
            return {'urls': all_urls, 'uuid': UUID}
        
        except Exception as e:
            logging.error(f"Failed to parse parent sitemap: {e}")
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
        
        if not urls:
            logging.info("No URLs to process; skipping child DAG triggers")
            return
        
        for i, url in enumerate(urls):
            # Rate limit child DAG triggers
            time.sleep(RATE_LIMIT_DELAY)
            child_run_id = f"triggered__{parent_run_id}_{i}"
            logging.info(f"Triggering shared_process_website_html for URL: {url} with run_id: {child_run_id}")
            
            trigger_dag_func(
                dag_id='shared_process_website_html',
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
    'shared_process_website_html',
    default_args=default_args,
    description='Process individual lowtouch.ai HTML page',
    schedule_interval=None,
    start_date=datetime(2025, 4, 16),
    catchup=False,
    max_active_runs=CHILD_DAG_CONCURRENCY,
    concurrency=CHILD_DAG_CONCURRENCY,
) as child_dag:

    def upload_html_to_agentvector(**context):
        try:
            conf = context.get('dag_run', {}).conf or {}
            url = conf.get('url')
            uuid = conf.get('uuid', UUID)  # Fallback to Airflow Variable
            
            if not url:
                logging.error("No 'url' provided in DAG run configuration")
                raise ValueError("Missing 'url' in DAG run configuration")
                
            # Fetch HTML content
            headers = {'User-Agent': USER_AGENT}
            logging.info(f"Fetching HTML content from: {url}")
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            html_content = response.text
            
            # Prepare agentvector API call
            encoded_url = urllib.parse.quote(url, safe='')
            agentvector_url = f"{AGENTVECTOR_BASE_URL}/{uuid}/{encoded_url}"
            logging.info(f"Uploading to agentvector: {agentvector_url}")
            
            # Send HTML content to agentvector
            api_response = session.post(agentvector_url, headers=headers, data=html_content, timeout=10)
            api_response.raise_for_status()
            logging.info(f"Successfully uploaded {url} to agentvector. Response: {api_response.text}")
        
        except Exception as e:
            logging.error(f"Failed to process HTML {url or 'unknown'}: {e}")
            raise

    upload_task = PythonOperator(
        task_id='upload_html',
        python_callable=upload_html_to_agentvector,
        provide_context=True,
    )

    upload_task
