from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task, task_group, Variable
from typing import List
from datetime import datetime, timedelta, timezone
import logging
from ollama import Client
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import base64
import json
import re
import html
import smtplib
import requests
import os
import pandas as pd
import numpy as np
from requests.auth import HTTPBasicAuth


# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# SMTP Configuration from Airflow Variables
SMTP_USER = Variable.get("ltai.v1.srembk.SMTP_USER")
SMTP_PASSWORD = Variable.get("ltai.v1.srembk.SMTP_PASSWORD")
SMTP_HOST = Variable.get("ltai.v1.srembk.SMTP_HOST", default_var="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("ltai.v1.srembk.SMTP_PORT", default_var="2525"))
SMTP_SUFFIX = Variable.get("ltai.v1.srembk.SMTP_FROM_SUFFIX", default_var="via lowtouch.ai <webmaster@ecloudcontrol.com>")
SENDER_EMAIL = Variable.get("ltai.v1.srembk.MBK_FROM_ADDRESS", default_var=SMTP_USER)
RECEIVER_EMAIL = Variable.get("ltai.v1.srembk.MBK_TO_ADDRESS", default_var=SENDER_EMAIL)

OLLAMA_HOST = Variable.get("ltai.v1.srembk.MBK_OLLAMA_HOST", "http://agentomatic:8000/")

# Prometheus Configuration
PROMETHEUS_URL = Variable.get("ltai.v1.srembk.MBK_PROMETHEUS_URL", "https://mbk-prometheus.lowtouchcloud.io")
PROMETHEUS_USER = Variable.get("ltai.v1.srembk.AGENT_PROMETHEUS_USER_MBK")
PROMETHEUS_PASSWORD = Variable.get("ltai.v1.srembk.AGENT_PROMETHEUS_PASSWORD_MBK")
auth = HTTPBasicAuth(PROMETHEUS_USER, PROMETHEUS_PASSWORD)

IST = timezone(timedelta(hours=5, minutes=30))

# IP-to-Name Mapping from queries.md
def get_node_name(instance):
    ip = instance.split(':')[0]
    mapping = Variable.get("ltai.v1.srembk.NODE_IP_MAPPING", default={}, deserialize_json=True)
    return mapping.get(ip, f"Unknown Node ({ip})")


# === Precise Date & Time Helpers (computed once per DAG run) ===
def get_daily_ranges():
    now = datetime.now(IST)
    today_11am = now.replace(hour=11, minute=0, second=0, microsecond=0)
    if now.time() < datetime(1,1,1,11,0,0).time():
        today_11am -= timedelta(days=1)
    yesterday_11am = today_11am - timedelta(days=1)
    day_before_11am = yesterday_11am - timedelta(days=1)

    current_start_dt = yesterday_11am
    current_end_dt = today_11am
    previous_start_dt = day_before_11am
    previous_end_dt = yesterday_11am

    current_start = yesterday_11am.astimezone(timezone.utc).timestamp()
    current_end = today_11am.astimezone(timezone.utc).timestamp()
    previous_start = day_before_11am.astimezone(timezone.utc).timestamp()
    previous_end = yesterday_11am.astimezone(timezone.utc).timestamp()

    current_period_str = f"{current_start_dt.strftime('%Y-%m-%d %H:%M')} to {current_end_dt.strftime('%Y-%m-%d %H:%M')} IST"
    previous_period_str = f"{previous_start_dt.strftime('%Y-%m-%d %H:%M')} to {previous_end_dt.strftime('%Y-%m-%d %H:%M')} IST"

    return (
        (current_start, current_end),
        (previous_start, previous_end),
        current_period_str,
        previous_period_str,
        current_start_dt,
        current_end_dt,
        previous_start_dt,
        previous_end_dt
    )

CURRENT_START, CURRENT_END = get_daily_ranges()[0]
PREVIOUS_START, PREVIOUS_END = get_daily_ranges()[1]
CURRENT_PERIOD = get_daily_ranges()[2]
PREVIOUS_PERIOD = get_daily_ranges()[3]
CURRENT_START_DT, CURRENT_END_DT, PREVIOUS_START_DT, PREVIOUS_END_DT = get_daily_ranges()[4:]

# For use in prompts (human-readable + precise)
YESTERDAY_FULL_RANGE = CURRENT_PERIOD  # Updated to new range

# For display only
YESTERDAY_DATE_STR = CURRENT_END_DT.strftime('%Y-%m-%d')  # End date for display

def query_prometheus_range(query: str, start: float, end: float, step: str = "5m"):
    url = f"{PROMETHEUS_URL}/api/v1/query_range"
    params = {
        "query": query,
        "start": start,
        "end": end,
        "step": step
    }
    logging.info(f"Querying Prometheus range: {query} from {start} to {end}")
    resp = requests.get(url, params=params, auth=auth, timeout=60, verify=True)
    resp.raise_for_status()
    data = resp.json()["data"]["result"]
    rows = []
    for result in data:
        metric = result["metric"]
        instance = metric.get("instance", "unknown")
        for timestamp, value_str in result["values"]:
            try:
                value = float(value_str)
            except ValueError:
                continue
            rows.append({
                "instance": instance,
                "timestamp": timestamp,
                "value": value,
                **metric  # Include other metrics like pod, mountpoint, etc.
            })
    df = pd.DataFrame(rows)
    logging.info(f"Fetched {len(df)} rows for query: {query}")
    return df

def query_prometheus_instant(query: str, time: float = None):
    url = f"{PROMETHEUS_URL}/api/v1/query"
    params = {"query": query}
    if time:
        params["time"] = time
    logging.info(f"Querying Prometheus instant: {query} at {time}")
    resp = requests.get(url, params=params, auth=auth, timeout=60, verify=True)
    resp.raise_for_status()
    data = resp.json()["data"]
    result_type = data["resultType"]
    results = data["result"]
    processed = []
    if result_type == "scalar":
        processed = [{"value": float(results[1])}]
    elif result_type == "vector":
        for r in results:
            metric = r["metric"]
            value = float(r["value"][1])
            processed.append({"value": value, **metric})
    logging.info(f"Fetched {len(processed)} results for instant query: {query}")
    return processed

       
def get_ai_response(prompt, conversation_history=None):
    try:
        logging.debug(f"Query received: {prompt}")
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")

        client = Client(host=OLLAMA_HOST)
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'SRE/AILAB:0.4'")

        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model='appz/sre/mbk:0.4',
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        if 'message' not in response or 'content' not in response['message']:
            logging.error("Response lacks expected 'message.content' structure")
            raise ValueError("Invalid response format from AI.")
        
        ai_content = response['message']['content'].strip()
        if not ai_content:
            logging.warning("AI returned empty content")
            raise ValueError("No response generated.")

        return ai_content
    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}")
        raise


#CPU Functions
def fetch_node_cpu_detailed(ti, key, start, end, date_str):
    """
    Fetches FULL metrics (CPU cores, usage, disk I/O) for TODAY and generates the detailed table.
    """
    # 1. Define Queries for Detailed Report
    queries = {
        "usage": '(1 - avg by (instance) (rate(node_cpu_seconds_total{origin_prometheus=~"",job=~"node-exporter",mode="idle"}[30s]))) * 100',
        "count": 'count by (instance) (node_cpu_seconds_total{mode="idle"})',
        "disk_read": 'max(rate(node_disk_read_bytes_total{origin_prometheus=~"",job=~"node-exporter"}[30s])) by (instance)',
        "disk_write": 'sum by (instance) (rate(node_disk_written_bytes_total[5m]))'
    }

    # 2. Fetch Data
    dfs = {}
    for name, q in queries.items():
        dfs[name] = query_prometheus_range(q, start, end, "5m")

    # 3. Process Data
    node_stats = {}

    def get_or_create_record(instance_label):
        ip = instance_label.split(':')[0]
        if ip not in node_stats:
            node_stats[ip] = {
                "ip": ip,
                "node_name": get_node_name(instance_label),
                "total_cpu": 0,
                "avg_cpu_pct": 0,
                "max_cpu_pct": 0,
                "current_cpu_pct": 0,
                "disk_read": 0,
                "disk_write": 0
            }
        return node_stats[ip]

    # Aggregate Cores
    if not dfs["count"].empty:
        for instance, group in dfs["count"].groupby('instance'):
            record = get_or_create_record(instance)
            record["total_cpu"] = int(group['value'].max())

    # Aggregate Usage
    if not dfs["usage"].empty:
        for instance, group in dfs["usage"].groupby('instance'):
            record = get_or_create_record(instance)
            record["avg_cpu_pct"] = group['value'].mean()
            record["max_cpu_pct"] = group['value'].max()
            record["current_cpu_pct"] = group['value'].iloc[-1]

    # Aggregate Disk I/O
    if not dfs["disk_read"].empty:
        for instance, group in dfs["disk_read"].groupby('instance'):
            record = get_or_create_record(instance)
            record["disk_read"] = group['value'].mean()
    
    if not dfs["disk_write"].empty:
        for instance, group in dfs["disk_write"].groupby('instance'):
            record = get_or_create_record(instance)
            record["disk_write"] = group['value'].mean()

    # 4. Generate Table
    sorted_stats = sorted(node_stats.values(), key=lambda x: x['node_name'])

    markdown = f"### CPU Utilization per Node (Date: {date_str})\n"
    markdown += "| Node Name | Total CPU (Cores) | Available CPU (Cores) | Avg CPU Utilization (%) | Max CPU Usage (%) | Current CPU Usage (%) | Disk Read (B/s) | Disk Write (B/s) |\n"
    markdown += "|-----------|-------------------|-----------------------|-------------------------|-------------------|-----------------------|-----------------|------------------|\n"

    final_data_list = []

    for stat in sorted_stats:
        total = stat["total_cpu"]
        avg_pct = stat["avg_cpu_pct"]
        
        available_cores = 0
        if total > 0:
            available_cores = total * (1 - (avg_pct / 100))

        row = {
            "instance_ip": stat["ip"],
            "node_name": stat["node_name"],
            "total_cpu": str(total),
            "available_cpu": f"{available_cores:.2f}",
            "avg_cpu": f"{avg_pct:.2f}",
            "max_cpu": f"{stat['max_cpu_pct']:.2f}",
            "current_cpu": f"{stat['current_cpu_pct']:.2f}",
            "disk_read": f"{stat['disk_read']:.2f}",
            "disk_write": f"{stat['disk_write']:.2f}"
        }
        
        markdown += f"| {row['node_name']} | {row['total_cpu']} | {row['available_cpu']} | {row['avg_cpu']} | {row['max_cpu']} | {row['current_cpu']} | {row['disk_read']} | {row['disk_write']} |\n"
        final_data_list.append(row)

    ti.xcom_push(key=key, value=markdown)
    ti.xcom_push(key=f"{key}_data", value=json.dumps(final_data_list))
    return markdown

def fetch_node_cpu_basic(ti, key, start, end):
    """
    Fetches ONLY Avg and Max CPU for YESTERDAY comparison.
    No table generation, just data push.
    """
    query_used = '(1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))) * 100'
    df_used = query_prometheus_range(query_used, start, end, "5m")

    data = []
    if not df_used.empty:
        grouped = df_used.groupby('instance')
        for instance, group in grouped:
            avg_cpu = round(group['value'].mean(), 2)
            max_cpu = round(group['value'].max(), 2)
            # We only need minimal data for comparison
            data.append({
                'instance_ip': instance.split(':')[0],
                'node_name': get_node_name(instance),
                'avg_cpu': avg_cpu,
                'max_cpu': max_cpu
            })
    
    logging.info(f"Generated Yesterday Data for XCom: {json.dumps(data, indent=2)}")
    # We push raw JSON only, no markdown needed for Yesterday
    ti.xcom_push(key=f"{key}_data", value=json.dumps(data))
    return f"Fetched basic CPU stats for {len(data)} nodes"

def node_cpu_today(ti, **context):
    date_display = CURRENT_END_DT.strftime('%Y-%m-%d')
    return fetch_node_cpu_detailed(ti, "node_cpu_today", CURRENT_START, CURRENT_END, date_display)

def node_cpu_yesterday(ti, **context):
    return fetch_node_cpu_basic(ti, "node_cpu_yesterday", PREVIOUS_START, PREVIOUS_END)

def node_cpu_today_vs_yesterday(ti, **context):
    # FIX 1: Safely retrieve XCom data with fallback to None
    json_today = ti.xcom_pull(key="node_cpu_today_data", task_ids="node_cpu_today")
    json_yesterday = ti.xcom_pull(key="node_cpu_yesterday_data", task_ids="node_cpu_yesterday")

    # FIX 2: Handle NoneType by defaulting to empty list, preventing json.loads crash
    data_today = json.loads(json_today) if json_today else []
    data_yesterday = json.loads(json_yesterday) if json_yesterday else []

    if not data_today and not data_yesterday:
        logging.warning("No CPU data found for Today or Yesterday. Generating empty report.")
        return "No data available for comparison."

    today_dict = {d['node_name']: d for d in data_today}
    yesterday_dict = {d['node_name']: d for d in data_yesterday}

    all_nodes = set(today_dict.keys()) | set(yesterday_dict.keys())
    comparison = []
    significant = []
    
    for node in all_nodes:
        t = today_dict.get(node, {})
        y = yesterday_dict.get(node, {})

        # Graceful handling of missing numeric values
        t_avg = float(t.get('avg_cpu', 0))
        t_max = float(t.get('max_cpu', 0))
        
        y_avg = float(y.get('avg_cpu', 0))
        y_max = float(y.get('max_cpu', 0))

        avg_diff = round(t_avg - y_avg, 4)
        max_diff = round(t_max - y_max, 4)

        comparison.append({
            'node_name': node,
            'instance_ip': t.get('instance_ip', y.get('instance_ip', 'Unknown')),
            'avg_cpu_p1': t_avg,
            'avg_cpu_p2': y_avg,
            'avg_cpu_diff': avg_diff,
            'max_cpu_p1': t_max,
            'max_cpu_p2': y_max,
            'max_cpu_diff': max_diff
        })

        if abs(max_diff) > 20:
            significant.append(f"{node} ({max_diff}%)")

    # Use Date strings if available in context, otherwise fallback to global defaults
    # Note: Assuming CURRENT_PERIOD/PREVIOUS_PERIOD are defined globally or in context
    p1_label = context.get('current_period', 'Today')
    p2_label = context.get('previous_period', 'Yesterday')

    markdown = f"### CPU Utilization Comparison - Today vs Yesterday ({p1_label} vs {p2_label})\n"
    markdown += "| Node Name | Instance IP | Avg CPU (%) - Today | Avg CPU (%) - Yest | Avg CPU Diff (%) | Max CPU (%) - Today | Max CPU (%) - Yest | Max CPU Diff (%) |\n"
    markdown += "|-----------|-------------|---------------------|--------------------|------------------|---------------------|--------------------|------------------|\n"
    
    for row in comparison:
        markdown += f"| {row['node_name']} | {row['instance_ip']} | {row['avg_cpu_p1']} | {row['avg_cpu_p2']} | {row['avg_cpu_diff']} | {row['max_cpu_p1']} | {row['max_cpu_p2']} | {row['max_cpu_diff']} |\n"

    summary = "No significant CPU changes." if not significant else f"Nodes with |max_diff| > 20%: {', '.join(significant)}"
    markdown += f"\n### Summary\n{summary}\n"

    ti.xcom_push(key="node_cpu_today_vs_yesterday", value=markdown)
    return markdown


#Memory Functions
def fetch_node_memory_detailed(ti, key, start, end, date_str):
    """
    Fetches Memory metrics for TODAY using range queries and generates a detailed table.
    """
    # 1. Define Queries (All Range)
    queries = {
        "total": 'node_memory_MemTotal_bytes / 1024 / 1024 / 1024',
        "avail": 'node_memory_MemAvailable_bytes / 1024 / 1024 / 1024'
    }

    # 2. Fetch Data
    dfs = {}
    for name, q in queries.items():
        dfs[name] = query_prometheus_range(q, start, end, "5m")

    # 3. Process Data
    node_stats = {}

    def get_or_create_record(instance_label):
        ip = instance_label.split(':')[0]
        if ip not in node_stats:
            node_stats[ip] = {
                "ip": ip,
                "node_name": get_node_name(instance_label),
                "total_mem_gb": 0,
                "avg_avail_gb": 0,
                "min_avail_gb": 0,
                "current_avail_gb": 0,
                "max_usage_pct": 0
            }
        return node_stats[ip]

    # Process Total Memory (Constant, take max to be safe)
    if not dfs["total"].empty:
        for instance, group in dfs["total"].groupby('instance'):
            record = get_or_create_record(instance)
            record["total_mem_gb"] = group['value'].max()

    # Process Available Memory
    if not dfs["avail"].empty:
        for instance, group in dfs["avail"].groupby('instance'):
            record = get_or_create_record(instance)
            record["avg_avail_gb"] = group['value'].mean()
            record["min_avail_gb"] = group['value'].min()
            record["current_avail_gb"] = group['value'].iloc[-1]

    # Calculate Max Usage %
    for stat in node_stats.values():
        total = stat["total_mem_gb"]
        min_avail = stat["min_avail_gb"]
        if total > 0:
            # Max Usage = 100 * (1 - (Min Available / Total))
            stat["max_usage_pct"] = 100 * (1 - (min_avail / total))
        else:
            stat["max_usage_pct"] = 0

    # 4. Generate Table
    sorted_stats = sorted(node_stats.values(), key=lambda x: x['max_usage_pct'], reverse=True)

    markdown = f"### Memory Utilization per Node (Date: {date_str})\n"
    markdown += "| Node Name | Instance IP | Total Memory (GB) | Avg Available (GB) | Current Available (GB) | Max Usage (%) |\n"
    markdown += "|-----------|-------------|-------------------|--------------------|------------------------|---------------|\n"

    final_data_list = []

    for stat in sorted_stats:
        row = {
            "node_name": stat["node_name"],
            "instance_ip": stat["ip"],
            "total_memory_gb": f"{stat['total_mem_gb']:.2f}",
            "avg_available_gb": f"{stat['avg_avail_gb']:.2f}",
            "current_available_gb": f"{stat['current_avail_gb']:.2f}",
            "max_usage_percent": f"{stat['max_usage_pct']:.2f}"
        }
        
        markdown += f"| {row['node_name']} | {row['instance_ip']} | {row['total_memory_gb']} | {row['avg_available_gb']} | {row['current_available_gb']} | {row['max_usage_percent']} |\n"
        final_data_list.append(row)

    ti.xcom_push(key=key, value=markdown)
    ti.xcom_push(key=f"{key}_data", value=json.dumps(final_data_list))
    return markdown

def fetch_node_memory_basic(ti, key, start, end):
    """
    Fetches ONLY data needed for YESTERDAY comparison using range queries.
    """
    queries = {
        "total": 'node_memory_MemTotal_bytes / 1024 / 1024 / 1024',
        "avail": 'node_memory_MemAvailable_bytes / 1024 / 1024 / 1024'
    }

    dfs = {}
    for name, q in queries.items():
        dfs[name] = query_prometheus_range(q, start, end, "5m")

    node_stats = {}

    # Helper to merge total and avail data
    def get_record(instance):
        if instance not in node_stats:
            node_stats[instance] = {"total": 0, "avg_avail": 0, "min_avail": 0}
        return node_stats[instance]

    if not dfs["total"].empty:
        for instance, group in dfs["total"].groupby('instance'):
            get_record(instance)["total"] = group['value'].max()

    if not dfs["avail"].empty:
        for instance, group in dfs["avail"].groupby('instance'):
            rec = get_record(instance)
            rec["avg_avail"] = group['value'].mean()
            rec["min_avail"] = group['value'].min()

    data = []
    for instance, stat in node_stats.items():
        total = stat["total"]
        min_avail = stat["min_avail"]
        max_usage = 0
        if total > 0:
            max_usage = 100 * (1 - (min_avail / total))
        
        data.append({
            'instance_ip': instance.split(':')[0],
            'node_name': get_node_name(instance),
            'total_memory_gb': round(total, 2),
            'avg_available_gb': round(stat["avg_avail"], 2),
            'max_usage_percent': round(max_usage, 2)
        })

    logging.info(f"Generated Yesterday Memory Data: {json.dumps(data, indent=2)}")
    ti.xcom_push(key=f"{key}_data", value=json.dumps(data))
    return f"Fetched basic Memory stats for {len(data)} nodes"

def node_memory_today(ti, **context):
    date_display = CURRENT_END_DT.strftime('%Y-%m-%d')
    return fetch_node_memory_detailed(ti, "node_memory_today", CURRENT_START, CURRENT_END, date_display)

def node_memory_yesterday(ti, **context):
    return fetch_node_memory_basic(ti, "node_memory_yesterday", PREVIOUS_START, PREVIOUS_END)

def node_memory_today_vs_yesterday(ti, **context):
    # FIX: Added task_ids explicitly so Airflow knows where to look
    raw_today = ti.xcom_pull(key="node_memory_today_data", task_ids="node_memory_today")
    raw_yesterday = ti.xcom_pull(key="node_memory_yesterday_data", task_ids="node_memory_yesterday")

    data_today = json.loads(raw_today) if raw_today else []
    data_yesterday = json.loads(raw_yesterday) if raw_yesterday else []

    today_dict = {d['node_name']: d for d in data_today}
    yesterday_dict = {d['node_name']: d for d in data_yesterday}

    all_nodes = set(today_dict.keys()) | set(yesterday_dict.keys())
    comparison = []
    significant = []
    
    for node in all_nodes:
        t = today_dict.get(node, {})
        y = yesterday_dict.get(node, {})

        t_total = float(t.get('total_memory_gb', 0))
        y_total = float(y.get('total_memory_gb', 0))
        
        t_avg = float(t.get('avg_available_gb', 0))
        y_avg = float(y.get('avg_available_gb', 0))
        
        t_max_usage = float(t.get('max_usage_percent', 0))
        y_max_usage = float(y.get('max_usage_percent', 0))

        avg_diff = round(t_avg - y_avg, 2)
        max_diff = round(t_max_usage - y_max_usage, 2)

        comparison.append({
            'node_name': node,
            'instance_ip': t.get('instance_ip', y.get('instance_ip', 'Unknown')),
            'total_mem_p1': t_total,
            'total_mem_p2': y_total,
            'avg_avail_p1': t_avg,
            'avg_avail_p2': y_avg,
            'avg_diff': avg_diff,
            'max_usage_p1': t_max_usage,
            'max_usage_p2': y_max_usage,
            'max_diff': max_diff
        })
        
        if abs(max_diff) > 20:
            significant.append(f"{node} ({max_diff}%)")

    p1_label = context.get('current_period', 'Today')
    p2_label = context.get('previous_period', 'Yesterday')

    markdown = f"### Memory Utilization Comparison - Today vs Yesterday ({p1_label} vs {p2_label})\n"
    markdown += "| Node Name | Instance IP | Total Mem (GB) | Avg Avail (GB) - Today | Avg Avail (GB) - Yest | Avg Diff (GB) | Max Usage (%) - Today | Max Usage (%) - Yest | Max Diff (%) |\n"
    markdown += "|-----------|-------------|----------------|------------------------|-----------------------|---------------|-----------------------|----------------------|--------------|\n"
    
    for row in comparison:
        markdown += f"| {row['node_name']} | {row['instance_ip']} | {row['total_mem_p1']} | {row['avg_avail_p1']} | {row['avg_avail_p2']} | {row['avg_diff']} | {row['max_usage_p1']} | {row['max_usage_p2']} | {row['max_diff']} |\n"

    summary = "No significant memory issues." if not significant else f"Nodes with |max_diff| > 20%: {', '.join(significant)}"
    markdown += f"\n### Summary\n{summary}\n"

    ti.xcom_push(key="node_memory_today_vs_yesterday", value=markdown)
    return markdown

#Disk Functions
def fetch_node_disk_detailed(ti, key, start, end, date_str):
    """
    Fetches Disk metrics for TODAY using range queries and generates a detailed table.
    Filters: Specific mountpoints (/ | /data | /var/lib/docker)
    """
    # 1. Define Queries
    # We fetch Size and Free; Used% is calculated in Python to ensure consistency
    queries = {
        "size": 'node_filesystem_size_bytes{mountpoint=~"/|/data|/var/lib/docker", fstype!~"tmpfs|overlay"} / 1024 / 1024 / 1024',
        "free": 'node_filesystem_free_bytes{mountpoint=~"/|/data|/var/lib/docker", fstype!~"tmpfs|overlay"} / 1024 / 1024 / 1024'
    }

    # 2. Fetch Data
    dfs = {}
    for name, q in queries.items():
        dfs[name] = query_prometheus_range(q, start, end, "5m")

    # 3. Process Data
    # Key = (instance_ip, mountpoint)
    disk_stats = {}

    def get_or_create_record(instance, mountpoint):
        ip = instance.split(':')[0]
        composite_key = (ip, mountpoint)
        if composite_key not in disk_stats:
            disk_stats[composite_key] = {
                "ip": ip,
                "node_name": get_node_name(instance),
                "mountpoint": mountpoint,
                "total_size_gb": 0,
                "free_space_gb": 0,
                "used_percent": 0
            }
        return disk_stats[composite_key]

    # Process Size
    if not dfs["size"].empty:
        # Group by instance AND mountpoint
        for (instance, mountpoint), group in dfs["size"].groupby(['instance', 'mountpoint']):
            record = get_or_create_record(instance, mountpoint)
            record["total_size_gb"] = group['value'].mean()

    # Process Free
    if not dfs["free"].empty:
        for (instance, mountpoint), group in dfs["free"].groupby(['instance', 'mountpoint']):
            record = get_or_create_record(instance, mountpoint)
            record["free_space_gb"] = group['value'].mean()

    # Calculate Used Percent
    for stat in disk_stats.values():
        total = stat["total_size_gb"]
        free = stat["free_space_gb"]
        if total > 0:
            stat["used_percent"] = 100 * (1 - (free / total))
        else:
            stat["used_percent"] = 0

    # 4. Generate Table
    # Sort: engine-master first, then others
    sorted_stats = sorted(disk_stats.values(), key=lambda x: (0 if 'engine-master' in x['node_name'] else 1, x['node_name'], x['mountpoint']))

    markdown = f"### Disk Utilization per Node (Date: {date_str})\n"
    markdown += "| Node Name | Instance IP | Mountpoint | Total Size (GB) | Free Space (GB) | Used (%) |\n"
    markdown += "|-----------|-------------|------------|-----------------|-----------------|----------|\n"

    final_data_list = []

    for stat in sorted_stats:
        row = {
            "node_name": stat["node_name"],
            "instance_ip": stat["ip"],
            "mountpoint": stat["mountpoint"],
            "total_size_gb": f"{stat['total_size_gb']:.2f}",
            "free_space_gb": f"{stat['free_space_gb']:.2f}",
            "used_percent": f"{stat['used_percent']:.2f}"
        }
        
        markdown += f"| {row['node_name']} | {row['instance_ip']} | {row['mountpoint']} | {row['total_size_gb']} | {row['free_space_gb']} | {row['used_percent']} |\n"
        final_data_list.append(row)

    ti.xcom_push(key=key, value=markdown)
    ti.xcom_push(key=f"{key}_data", value=json.dumps(final_data_list))
    return markdown

def fetch_node_disk_basic(ti, key, start, end):
    """
    Fetches ONLY data needed for YESTERDAY comparison using range queries.
    Filters: General regex (ext*|xfs) excluding pods.
    """
    queries = {
        "size": 'node_filesystem_size_bytes{fstype=~"ext.*|xfs",mountpoint !~".*pod.*"} / 1024 / 1024 / 1024',
        "free": 'node_filesystem_free_bytes{fstype=~"ext.*|xfs",mountpoint !~".*pod.*"} / 1024 / 1024 / 1024'
    }

    dfs = {}
    for name, q in queries.items():
        dfs[name] = query_prometheus_range(q, start, end, "5m")

    disk_stats = {}

    def get_record(instance, mountpoint):
        composite_key = (instance, mountpoint)
        if composite_key not in disk_stats:
            disk_stats[composite_key] = {
                "instance": instance,
                "mountpoint": mountpoint, 
                "total": 0, 
                "free": 0
            }
        return disk_stats[composite_key]

    # Process Size
    if not dfs["size"].empty:
        for (instance, mountpoint), group in dfs["size"].groupby(['instance', 'mountpoint']):
            get_record(instance, mountpoint)["total"] = group['value'].mean()

    # Process Free
    if not dfs["free"].empty:
        for (instance, mountpoint), group in dfs["free"].groupby(['instance', 'mountpoint']):
            get_record(instance, mountpoint)["free"] = group['value'].mean()

    data = []
    for stat in disk_stats.values():
        total = stat["total"]
        free = stat["free"]
        used_percent = 0
        
        if total > 0:
            used_percent = 100 * (1 - (free / total))
        
        data.append({
            'instance_ip': stat['instance'].split(':')[0],
            'node_name': get_node_name(stat['instance']),
            'mountpoint': stat['mountpoint'],
            'total_size_gb': round(total, 2),
            'free_space_gb': round(free, 2),
            'used_percent': round(used_percent, 2)
        })

    logging.info(f"Generated Yesterday Disk Data: {json.dumps(data, indent=2)}")
    ti.xcom_push(key=f"{key}_data", value=json.dumps(data))
    return f"Fetched basic Disk stats for {len(data)} mountpoints"

# === DAG Tasks ===

def node_disk_today(ti, **context):
    date_display = CURRENT_END_DT.strftime('%Y-%m-%d')
    return fetch_node_disk_detailed(ti, "node_disk_today", CURRENT_START, CURRENT_END, date_display)

def node_disk_yesterday(ti, **context):
    return fetch_node_disk_basic(ti, "node_disk_yesterday", PREVIOUS_START, PREVIOUS_END)

def node_disk_today_vs_yesterday(ti, **context):
    # FIX: Added task_ids explicitly
    raw_today = ti.xcom_pull(key="node_disk_today_data", task_ids="node_disk_today")
    raw_yesterday = ti.xcom_pull(key="node_disk_yesterday_data", task_ids="node_disk_yesterday")

    data_today = json.loads(raw_today) if raw_today else []
    data_yesterday = json.loads(raw_yesterday) if raw_yesterday else []

    today_dict = {(d['node_name'], d['mountpoint']): d for d in data_today}
    yesterday_dict = {(d['node_name'], d['mountpoint']): d for d in data_yesterday}

    all_keys = set(today_dict.keys()) | set(yesterday_dict.keys())
    comparison = []
    significant = []
    
    for key in all_keys:
        node, mountpoint = key
        t = today_dict.get(key, {})
        y = yesterday_dict.get(key, {})

        t_used = float(t.get('used_percent', 0))
        y_used = float(y.get('used_percent', 0))
        used_diff = round(t_used - y_used, 2)

        comparison.append({
            'node_name': node,
            'instance_ip': t.get('instance_ip', y.get('instance_ip', 'Unknown')),
            'mountpoint': mountpoint,
            'used_p1': t_used,
            'used_p2': y_used,
            'diff': used_diff
        })
        
        if abs(used_diff) > 20:
            significant.append(f"{node} ({mountpoint}: {used_diff}%)")

    p1_label = context.get('current_period', 'Today')
    p2_label = context.get('previous_period', 'Yesterday')

    markdown = f"### Disk Utilization Comparison - Node Level ({p1_label} vs {p2_label})\n"
    markdown += "| Node Name | Instance IP | Mountpoint | Used (%) - Today | Used (%) - Yest | Diff (%) |\n"
    markdown += "|-----------|-------------|------------|------------------|-----------------|----------|\n"
    
    comparison.sort(key=lambda x: (x['node_name'], x['mountpoint']))

    for row in comparison:
        markdown += f"| {row['node_name']} | {row['instance_ip']} | {row['mountpoint']} | {row['used_p1']} | {row['used_p2']} | {row['diff']} |\n"

    summary = "No significant disk issues." if not significant else f"Entries with |used_diff| > 20%: {', '.join(significant)}"
    markdown += f"\n### Summary\n{summary}\n"

    ti.xcom_push(key="node_disk_today_vs_yesterday", value=markdown)
    return markdown

# Node Readiness Check
def fetch_node_readiness(ti, **context):
    query = 'up{job=~"(kubernetes-cadvisor|node-exporter|backup_metrics)"}'
    results = query_prometheus_instant(query)
    data = []
    for r in results:
        instance = r.get('instance', 'Unknown Instance')
        job = r.get('job', 'Unknown Job')
        status = "Ready" if r['value'] == 1 else "Not Ready"
        if job == 'node-exporter':
            instance = instance.split(':')[0]  # Strip port
            node_name = get_node_name(instance)
            instance_ip = instance
        else:
            node_name = instance # For other jobs, use full instance
        data.append({
            'node_name': node_name,
            'job': job,
            'status': status
        })

    markdown = "### Node Readiness Check\n"
    markdown += "| Instance | Job | Status |\n"
    markdown += "|-------------|-----------|--------|\n"
    for row in data:
        markdown += f"| {row['node_name']} | {row['job']} | {row['status']} |\n"

    ti.xcom_push(key="node_readiness_check", value=markdown)
    return markdown


# Pod Restart Functions
def fetch_pod_restart(ti, key, start, end, period_str, date_str):
    """
    Fetches Pod Restart counts using range queries for specific namespaces.
    Reports the Total Restart Count at the end of the period.
    """
    # 1. Fetch Namespaces from Airflow Variable
    try:
        namespaces_list = json.loads(
            Variable.get("ltai.v1.srembk.pod.namespaces", '["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]')
        )
        
    except Exception as e:
        logging.warning(
            "Failed to parse Airflow Variable 'ltai.v1.srembk.pod.namespaces'. "
            "Reason: %s. Falling back to hard-coded default namespaces.", str(e))
        namespaces_list = ["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]

    # Create regex for Prometheus (e.g., "alpha-prod|tipreprod-prod")
    namespace_regex = "|".join(namespaces_list)

    # 2. Define Query
    query = f'increase(kube_pod_container_status_restarts_total{{namespace=~"{namespace_regex}"}}[24h]) > 0'

    # 3. Fetch Range Data
    # We fetch the range to ensure valid data existence over the period, 
    # but we primarily care about the count at the END of the period.
    df = query_prometheus_range(query, start, end, "5m")

    data = []
    if not df.empty:
        # Group by pod and container to handle the time series
        grouped = df.groupby(['pod', 'container'])
        for (pod, container), group in grouped:
            # We take the last value in the series (status at the end of the window)
            last_value = int(group['value'].iloc[-1])
            
            # Double check > 0 (redundant if query has > 0, but safe)
            if last_value > 0:
                data.append({
                    'pod': pod,
                    'container': container,
                    'restart_count': last_value
                })

    # 4. Generate Markdown
    markdown = f"### Pod Restart Count (Date: {date_str})\n"
    
    if not data:
        markdown += "No pod restarts detected.\n"
    else:
        # Sort by restart count descending
        data.sort(key=lambda x: x['restart_count'], reverse=True)
        
        markdown += "| Pod | Container | Restart Count |\n"
        markdown += "|-----|-----------|---------------|\n"
        for row in data:
            markdown += f"| {row['pod']} | {row['container']} | {row['restart_count']} |\n"

    # 5. Push XCom
    ti.xcom_push(key=key, value=markdown)
    ti.xcom_push(key=f"{key}_data", value=json.dumps(data))
    return markdown

def pod_restart_today(ti, **context):
    date_display = CURRENT_END_DT.strftime('%Y-%m-%d')
    return fetch_pod_restart(ti, "pod_restart_today", CURRENT_START, CURRENT_END, CURRENT_PERIOD, date_display)


def fetch_kubernetes_eol(ti, key, start, end):
    """
    Fetches K8s version and EOL details, formatting strictly as requested.
    """
    # 1. Get Current Version from Prometheus
    # Query returns something like: kubernetes_build_info{git_version="v1.29.2", ...} 1
    query = 'count by (git_version) (kubernetes_build_info)'
    df = query_prometheus_range(query, start, end, "1h")
    
    current_version_full = "Unknown"
    short_version = "Unknown"

    if not df.empty:
        # Extract the version string (e.g., "v1.29.2")
        # Assuming the dataframe has the metric labels available. 
        # If your 'query_prometheus_range' returns a dataframe with 'git_version' column:
        if 'git_version' in df.columns:
             current_version_full = df['git_version'].iloc[-1]
        # Fallback: sometimes the metric label is packed in a 'metric' dictionary column
        elif 'metric' in df.columns:
             metric_dict = df['metric'].iloc[-1]
             current_version_full = metric_dict.get('git_version', 'Unknown')
    
    # Parse "v1.29.2" -> "1.29"
    if current_version_full.startswith('v'):
        parts = current_version_full[1:].split('.')
        if len(parts) >= 2:
            short_version = f"{parts[0]}.{parts[1]}"

    # 2. Fetch Official EOL Data
    eol_data = []
    try:
        resp = requests.get("https://endoflife.date/api/kubernetes.json", timeout=10)
        resp.raise_for_status()
        eol_data = resp.json()
    except Exception as e:
        logging.error(f"Failed to fetch EOL data: {e}")
        return "Error fetching Kubernetes lifecycle data."

    # 3. Analyze Versions
    current_info = {}
    next_info = {}
    
    # Sort descending (newest first)
    eol_data.sort(key=lambda x: float(x['cycle']), reverse=True)
    
    for i, release in enumerate(eol_data):
        if release['cycle'] == short_version:
            current_info = release
            # The "next" version is the one appearing before this in the sorted list
            if i > 0:
                next_info = eol_data[i-1]
            break

    # 4. Format Output
    # Helper to calculate days
    def get_days_remaining(date_str):
        if not date_str or date_str is False: 
            return "Unknown"
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            today = datetime.now().date()
            delta = (target_date - today).days
            return delta
        except:
            return "Unknown"

    # Extract Dates
    active_date = current_info.get('support', 'Unknown') # 'support' usually maps to Active Support in this API
    maint_date = current_info.get('eol', 'Unknown')      # 'eol' maps to Maintenance/End of Life

    active_days = get_days_remaining(active_date)
    maint_days = get_days_remaining(maint_date)

    # Build String
    output = f"### Kubernetes Current Version EOL Details and Next Supported Kubernetes Version\n"
    output += f"Current Kubernetes Version: {current_version_full}\n"
    
    # Active Support Line
    if isinstance(active_days, int):
        status_str = "days remaining" if active_days >= 0 else "days ago"
        output += f"- Active Support Ends: {active_date} ({abs(active_days)} {status_str})\n"
    else:
        output += f"- Active Support Ends: {active_date}\n"

    # Maintenance Support Line
    if isinstance(maint_days, int):
        status_str = "days remaining" if maint_days >= 0 else "days ago"
        output += f"- Maintenance Support Ends: {maint_date} ({abs(maint_days)} {status_str})\n"
    else:
        output += f"- Maintenance Support Ends: {maint_date}\n"

    # Next Version Section
    output += "Next Supported Kubernetes Version\n"
    next_ver = next_info.get('cycle', 'None')
    output += f"- Next Version: {next_ver}\n"

    ti.xcom_push(key=key, value=output)
    return output

def kubernetes_eol_and_next_version(ti, **context):
    return fetch_kubernetes_eol(ti, "kubernetes_eol_and_next_version", CURRENT_START, CURRENT_END)


def fetch_microk8s_expiry(ti, **context):
    query = 'microk8s_cert_expiry'
    
    # Fetch results
    try:
        results = query_prometheus_instant(query)
    except Exception as e:
        logging.error(f"Error fetching microk8s expiry: {e}")
        results = []

    data = []
    for r in results:
        cert = r.get('cert', 'Unknown Cert')
        instance = r.get('instance', 'Unknown Instance')
        try:
            expiry_ts = float(r['value'])
            expiry_date = datetime.fromtimestamp(expiry_ts).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            expiry_date = "Invalid Timestamp"

        data.append({
            'cert': cert,
            'instance': instance,
            'expiry_date': expiry_date
        })

    # Generate Markdown
    markdown = "### MicroK8s Master Node Certificate Expiry Check\n"
    markdown += "| Certificate | Instance | Expiry Date |\n"
    markdown += "|------------|----------|-------------|\n"
    
    if not data:
        # Display "No Data Available" row if list is empty
        markdown += "| No Data Available | - | - |\n"
    else:
        for row in data:
            markdown += f"| {row['cert']} | {row['instance']} | {row['expiry_date']} |\n"

    ti.xcom_push(key="microk8s_expiry_check", value=markdown)
    return markdown


def check_ssl_cert_expiry(ti, **context):
    """
    Fetches SSL certificate expiry timestamps from Prometheus Blackbox exporter
    and generates a status table sorted by earliest expiry.
    """
    query = 'probe_ssl_earliest_cert_expiry{job="blackbox"}'
    
    # 1. Fetch results from Prometheus
    try:
        results = query_prometheus_instant(query)
    except Exception as e:
        logging.error(f"Error fetching SSL expiry metrics: {e}")
        results = []

    data = []
    current_time = datetime.now()

    # 2. Process Data
    for r in results:
        instance = r.get('instance', 'Unknown URL')
        
        try:
            # Convert Unix timestamp to readable date
            expiry_ts = float(r['value'])
            expiry_dt = datetime.fromtimestamp(expiry_ts)
            expiry_date_str = expiry_dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Calculate days remaining for sorting/urgency
            days_remaining = (expiry_dt - current_time).days
            
        except (ValueError, TypeError):
            expiry_date_str = "Invalid Timestamp"
            days_remaining = 9999 # Push to bottom if invalid

        data.append({
            'instance': instance,
            'expiry_date': expiry_date_str,
            'days_remaining': days_remaining
        })

    # 3. Sort by urgency (Earliest expiry first)
    data.sort(key=lambda x: x['days_remaining'])

    # 4. Generate Markdown Table
    markdown = "### SSL Certificate Expiry Check\n"
    markdown += "| Instance | Expiry Date | Days Remaining |\n"
    markdown += "|----------|-------------|----------------|\n"
    
    if not data:
        markdown += "| No SSL Data Found | - | - |\n"
    else:
        for row in data:
            # Optional: Add alert emoji if expiry is soon (e.g., < 30 days)
            status_icon = "⚠️" if row['days_remaining'] < 30 else ""
            markdown += f"| {row['instance']} | {row['expiry_date']} | {row['days_remaining']} {status_icon} |\n"

    # 5. Push to XCom
    ti.xcom_push(key="ssl_cert_expiry_check", value=markdown)
    return markdown

#Pod Metrics    
def fetch_pod_data_for_period(namespaces, start, end, period_label):
    """
    Fetches Pod data. 
    - Uses INSTANT queries for Status (Running/Problematic) to avoid stale historical data.
    - Uses RANGE queries for CPU/Memory to get averages.
    """
    results = []
    
    # 1. Status Queries (Use INSTANT at 'end' time)
    # This ensures we match exactly what you see in Prometheus right now.
    
    for ns in namespaces:
        ns = ns.strip() # Safety trim
        
        # A. Running Pods (Instant)
        q_running = f'count(kube_pod_status_phase{{namespace="{ns}", phase="Running"}} == 1)'
        try:
            # We use the instant helper we defined earlier (returns list of dicts)
            run_res = query_prometheus_instant(q_running, time=end)
            total_running = int(run_res[0]['value']) if run_res else 0
        except Exception as e:
            logging.error(f"Error fetching running pods for {ns}: {e}")
            total_running = 0

        # B. Problematic Pods (Instant)
        # We only want pods that are CURRENTLY Failed/Pending/Unknown
        q_problem = f'sum by (pod, phase) (kube_pod_status_phase{{namespace="{ns}", phase=~"Failed|Pending|Unknown"}} == 1)'
        problem_list = []
        try:
            prob_res = query_prometheus_instant(q_problem, time=end)
            # Result format: [{'metric': {'pod': 'x', 'phase': 'Pending'}, 'value': 1.0}, ...]
            # Or if flattened by helper: [{'pod': 'x', 'phase': 'Pending', 'value': 1.0}]
            
            for r in prob_res:
                # Helper might return flattened dict or nested metric dict
                pod_name = r.get('pod') or r.get('metric', {}).get('pod')
                phase = r.get('phase') or r.get('metric', {}).get('phase')
                
                # Double check value is 1 (Active)
                if r['value'] == 1:
                    problem_list.append(f"{pod_name} ({phase})")
                    
        except Exception as e:
            logging.error(f"Error fetching problematic pods for {ns}: {e}")

        problematic_str = "\n".join(problem_list) if problem_list else "No problematic pods"

        # C. CPU Metrics (Range - Keep as is for Averages)
        q_cpu = f'sum by (pod) (rate(container_cpu_usage_seconds_total{{image!="", container!="POD", namespace="{ns}"}}[5m]))'
        df_cpu = query_prometheus_range(q_cpu, start, end, "5m")
        
        cpu_data = []
        if not df_cpu.empty:
            if 'metric' in df_cpu.columns:
                 grouped = df_cpu.groupby(df_cpu['metric'].apply(lambda x: x.get('pod')))
            else:
                 grouped = df_cpu.groupby(['pod'])
            
            for pod_key, group in grouped:
                # Handle tuple keys from groupby
                pod = pod_key[0] if isinstance(pod_key, tuple) else pod_key
                if not pod: continue
                
                cpu_data.append({
                    'pod': str(pod),
                    'avg': round(float(group['value'].mean()), 4),
                    'max': round(float(group['value'].max()), 4),
                    'current': round(float(group['value'].iloc[-1]), 4)
                })

        # D. Memory Metrics (Range)
        q_mem = f'sum by (pod) (container_memory_working_set_bytes{{image!="", namespace="{ns}"}}) / 1024 / 1024 / 1024'
        df_mem = query_prometheus_range(q_mem, start, end, "5m")
        
        mem_data = []
        if not df_mem.empty:
            if 'metric' in df_mem.columns:
                 grouped = df_mem.groupby(df_mem['metric'].apply(lambda x: x.get('pod')))
            else:
                 grouped = df_mem.groupby(['pod'])

            for pod_key, group in grouped:
                pod = pod_key[0] if isinstance(pod_key, tuple) else pod_key
                if not pod: continue
                
                mem_data.append({
                    'pod': str(pod),
                    'avg': round(float(group['value'].mean()), 4),
                    'max': round(float(group['value'].max()), 4),
                    'current': round(float(group['value'].iloc[-1]), 4)
                })

        results.append({
            "namespace": ns,
            "period": period_label,
            "total_running": total_running,
            "problematic_pods": problematic_str,
            "cpu_data": cpu_data,
            "memory_data": mem_data
        })
        
    return results

# === AIRFLOW TASKS ===

def pod_details_today(ti, **context):
    # 1. Get Namespaces & Clean them
    try:
        raw = Variable.get("ltai.v1.srembk.pod.namespaces", '["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]')
        namespaces_raw = json.loads(raw)
        # Ensure list and strip whitespace
        namespaces = [str(n).strip() for n in namespaces_raw]
    except:
        namespaces = ["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]

    # 2. Fetch
    data = fetch_pod_data_for_period(namespaces, CURRENT_START, CURRENT_END, "last 24 hours")
    
    # 3. Build Map for lookup
    # Key is the stripped namespace
    data_map = {d['namespace']: d for d in data}
    
    sections = []
    
    for ns in namespaces:
        # Debug log if missing
        if ns not in data_map: 
            logging.warning(f"Namespace {ns} found in variable but no data returned from fetch function.")
            continue
            
        d = data_map[ns]
        
        # Header + Counts
        sections.append(f"### Namespace: `{ns}`")
        sections.append(f"**Running Pods**: {d['total_running']} | **Problematic Pods**: {d['problematic_pods']}\n")
        
        # CPU
        if d['cpu_data']:
            sections.append(f"#### CPU Utilization (Last 24h)")
            sections.append("| Pod | Avg (cores) | Max (cores) | Current (cores) |")
            sections.append("|-----|-------------|-------------|-----------------|")
            for row in sorted(d['cpu_data'], key=lambda x: x['avg'], reverse=True):
                sections.append(f"| {row['pod']} | {row['avg']} | {row['max']} | {row['current']} |")
            sections.append("")
        else:
             sections.append("_No CPU data available for this namespace._\n")

        # Memory
        if d['memory_data']:
            sections.append(f"#### Memory Utilization (Last 24h)")
            sections.append("| Pod | Avg (GB) | Max (GB) | Current (GB) |")
            sections.append("|-----|----------|----------|-------------|")
            for row in sorted(d['memory_data'], key=lambda x: x['avg'], reverse=True):
                sections.append(f"| {row['pod']} | {row['avg']} | {row['max']} | {row['current']} |")
            sections.append("")
        else:
             sections.append("_No Memory data available for this namespace._\n")
        
        sections.append("---")

    markdown = "\n".join(sections)
    
    ti.xcom_push(key="pod_details_today", value=markdown)
    ti.xcom_push(key="pod_details_today_data", value=json.dumps(data))
    return markdown

def pod_details_yesterday(ti, **context):
    try:
        raw = Variable.get("ltai.v1.srembk.pod.namespaces", '["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]')
        namespaces = json.loads(raw)
    except:
        namespaces = ["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]


    # Fetch Data
    data = fetch_pod_data_for_period(namespaces, PREVIOUS_START, PREVIOUS_END, "yesterday")
    
    # We only need the JSON for comparison
    ti.xcom_push(key="pod_details_yesterday_data", value=json.dumps(data))
    return "Fetched Pod Details for Yesterday"

def pod_details_today_vs_yesterday(ti, **context):
    # FIX: Added task_ids explicitly
    raw_today = ti.xcom_pull(key="pod_details_today_data", task_ids="pod_details_today")
    raw_yesterday = ti.xcom_pull(key="pod_details_yesterday_data", task_ids="pod_details_yesterday")

    data_today = json.loads(raw_today) if raw_today else []
    data_yesterday = json.loads(raw_yesterday) if raw_yesterday else []
    
    today_map = {d['namespace']: d for d in data_today}
    yest_map = {d['namespace']: d for d in data_yesterday}
    
    all_ns = set(today_map.keys()) | set(yest_map.keys())
    
    try:
        raw = Variable.get("ltai.v1.srembk.pod.namespaces", '["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]')
        ordered_ns = json.loads(raw)
    except:
        ordered_ns = sorted(list(all_ns))
        
    sections = []
    
    for ns in ordered_ns:
        if ns not in all_ns: continue
        
        t_ns = today_map.get(ns, {'cpu_data': [], 'memory_data': []})
        y_ns = yest_map.get(ns, {'cpu_data': [], 'memory_data': []})
        
        # --- CPU Comparison ---
        t_cpu = {x['pod']: x for x in t_ns['cpu_data']}
        y_cpu = {x['pod']: x for x in y_ns['cpu_data']}
        all_pods_cpu = set(t_cpu.keys()) | set(y_cpu.keys())
        
        cpu_rows = []
        for pod in all_pods_cpu:
            t = t_cpu.get(pod, {'avg': 0, 'max': 0})
            y = y_cpu.get(pod, {'avg': 0, 'max': 0})
            
            diff_avg = round(t['avg'] - y['avg'], 4)
            diff_max = round(t['max'] - y['max'], 4)
            
            cpu_rows.append({
                "pod": pod,
                "t_avg": t['avg'], "y_avg": y['avg'], "diff_avg": diff_avg,
                "t_max": t['max'], "y_max": y['max'], "diff_max": diff_max
            })

        # --- Memory Comparison ---
        t_mem = {x['pod']: x for x in t_ns['memory_data']}
        y_mem = {x['pod']: x for x in y_ns['memory_data']}
        all_pods_mem = set(t_mem.keys()) | set(y_mem.keys())
        
        mem_rows = []
        for pod in all_pods_mem:
            t = t_mem.get(pod, {'avg': 0, 'max': 0})
            y = y_mem.get(pod, {'avg': 0, 'max': 0})
            
            diff_avg = round(t['avg'] - y['avg'], 4)
            diff_max = round(t['max'] - y['max'], 4)
            
            mem_rows.append({
                "pod": pod,
                "t_avg": t['avg'], "y_avg": y['avg'], "diff_avg": diff_avg,
                "t_max": t['max'], "y_max": y['max'], "diff_max": diff_max
            })
            
        # Build Section
        sections.append(f"### {ns} — Pod CPU & Memory Comparison")
        
        # CPU Table
        sections.append("#### CPU Changes")
        sections.append("| Pod | Today Avg | Yest Avg | Diff | Today Max | Yest Max | Max Diff |")
        sections.append("|-----|-----------|----------|------|-----------|----------|----------|")
        # Sort by Max Diff magnitude
        for row in sorted(cpu_rows, key=lambda x: abs(x['diff_max']), reverse=True):
             sections.append(f"| {row['pod']} | {row['t_avg']} | {row['y_avg']} | {row['diff_avg']} | {row['t_max']} | {row['y_max']} | {row['diff_max']} |")

        sections.append("")
        
        # Memory Table
        sections.append("#### Memory Changes")
        sections.append("| Pod | Today Avg (GB) | Yest Avg | Diff | Today Max (GB) | Yest Max | Max Diff |")
        sections.append("|-----|----------------|----------|------|----------------|----------|----------|")
        for row in sorted(mem_rows, key=lambda x: abs(x['diff_max']), reverse=True):
             sections.append(f"| {row['pod']} | {row['t_avg']} | {row['y_avg']} | {row['diff_avg']} | {row['t_max']} | {row['y_max']} | {row['diff_max']} |")
             
        sections.append("\n---\n")

    result = "\n".join(sections)
    ti.xcom_push(key="pod_details_today_vs_yesterday", value=result)
    return result

def summarize_node_cpu(ti, **context):
    # Pull Data
    today = ti.xcom_pull(key="node_cpu_today", task_ids="node_cpu_today") or "No data"
    comparison = ti.xcom_pull(key="node_cpu_today_vs_yesterday", task_ids="node_cpu_today_vs_yesterday") or "No comparison"

    # Specific Prompt
    prompt = f"""You are the SRE MBK agent. Analyze the Node CPU metrics.
    
    ### Data
    - Today's Status: {today}
    - Comparison (Today vs Yesterday): {comparison}

    ### Task
    Write a concise summary paragraph highlighting:
    1. Overall CPU health (Safe/High Load).
    2. Any specific nodes showing significant spikes (>20% change).
    3. Recommendations if critical.
    """
    
    response = get_ai_response(prompt)
    ti.xcom_push(key="summary_cpu", value=response)
    return response

def summarize_node_memory(ti, **context):
    # Pull Data
    today = ti.xcom_pull(key="node_memory_today", task_ids="node_memory_today") or "No data"
    comparison = ti.xcom_pull(key="node_memory_today_vs_yesterday", task_ids="node_memory_today_vs_yesterday") or "No comparison"

    # Specific Prompt
    prompt = f"""You are the SRE MBK agent. Analyze the Node Memory metrics.
    
    ### Data
    - Today's Status: {today}
    - Comparison (Today vs Yesterday): {comparison}

    ### Task
    Write a concise summary paragraph highlighting:
    1. Nodes with low available memory.
    2. Significant changes in memory usage compared to yesterday.
    """
    
    response = get_ai_response(prompt)
    ti.xcom_push(key="summary_memory", value=response)
    return response

def summarize_node_disk(ti, **context):
    # Pull Data
    today = ti.xcom_pull(key="node_disk_today", task_ids="node_disk_today") or "No data"
    comparison = ti.xcom_pull(key="node_disk_today_vs_yesterday", task_ids="node_disk_today_vs_yesterday") or "No comparison"

    # Specific Prompt
    prompt = f"""You are the SRE MBK agent. Analyze the Node Disk metrics.
    
    ### Data
    - Today's Status: {today}
    - Comparison (Today vs Yesterday): {comparison}

    ### Task
    Write a concise summary paragraph highlighting:
    1. Nodes with high disk utilization.
    2. Any significant changes in disk usage compared to yesterday.
    """
    
    response = get_ai_response(prompt)
    ti.xcom_push(key="summary_disk", value=response)
    return response

def summarize_pod_restarts(ti, **context):
    # Pull Data
    today = ti.xcom_pull(key="pod_restart_today", task_ids="pod_restart_today") or "No data"

    # Specific Prompt
    prompt = f"""You are the SRE MBK agent. Analyze the Pod Restart metrics.
    
    ### Data
    - Today's Pod Restarts: {today}

    ### Task
    Write a concise summary paragraph highlighting:
    1. Any pods with restart counts.
    """
    
    response = get_ai_response(prompt)
    ti.xcom_push(key="summary_pod_restarts", value=response)
    return response


def summarize_health_checks(ti, **context):
    # Pull Data
    readiness = ti.xcom_pull(key="node_readiness_check", task_ids="node_readiness_check") or "No data"
    k8s_eol = ti.xcom_pull(key="kubernetes_eol_and_next_version", task_ids="kubernetes_eol_and_next_version") or "No data"
    microk8s = ti.xcom_pull(key="microk8s_expiry_check", task_ids="microk8s_expiry_check") or "No data"
    ssl= ti.xcom_pull(key="ssl_cert_expiry_check", task_ids="ssl_cert_expiry_check") or "No data"

    # Specific Prompt
    prompt = f"""You are the SRE MBK agent. Analyze the Health Check metrics.
    
    ### Data
    - Node Readiness: {readiness}
    - Kubernetes EOL: {k8s_eol}
    - MicroK8s Expiry: {microk8s}
    - SSL Certificate Expiry: {ssl}

    ### Task
    Write a concise summary paragraph highlighting:
    1. Any nodes not ready.
    2. Kubernetes version status and upcoming EOL.
    3. MicroK8s certificate expiry status.
    """
    
    response = get_ai_response(prompt)
    ti.xcom_push(key="summary_health_checks", value=response)
    return response


def overall_summary(ti, **context):
    """
    Compiles the individual AI-generated summaries into one final SRE Report.
    """
    # 1. Pull the specific summaries from their respective tasks
    # Default to a placeholder string if the upstream task was skipped/failed
    cpu_sum = ti.xcom_pull(key="summary_cpu", task_ids="summarize_node_cpu") or "⚠️ CPU summary not available."
    mem_sum = ti.xcom_pull(key="summary_memory", task_ids="summarize_node_memory") or "⚠️ Memory summary not available."
    disk_sum = ti.xcom_pull(key="summary_disk", task_ids="summarize_node_disk") or "⚠️ Disk summary not available."
    pos_restart_sum = ti.xcom_pull(key="summary_pod_restarts", task_ids="summarize_pod_restarts") or "⚠️ Pod Restart summary not available."
    health_sum = ti.xcom_pull(key="summary_health_checks", task_ids="summarize_health_checks") or "⚠️ Health summary not available."

    date_str = context.get('ds', 'N/A')

    # 2. Construct Final Markdown Report
    final_report = f"""

## 1. 🧠 Node CPU Analysis
{cpu_sum}

## 2. 💾 Node Memory Analysis
{mem_sum}

## 3. 💿 Disk & Storage Analysis
{disk_sum}

## 4. 📦 Pod Restarts Summary
{pos_restart_sum}

## 5. 🏥 Health Status
{health_sum}

"""

    # 3. Push final report to XCom
    ti.xcom_push(key="overall_summary", value=final_report)
    
    return final_report


# === Compile SRE Report ===
def compile_sre_report(ti, **context):
    def xp(key, default="No data"): return ti.xcom_pull(key=key) or default
    
    # 1. Fetching Data
    node_cpu = ti.xcom_pull(key="node_cpu_today", task_ids="node_cpu_today") or "No data"
    node_memory = ti.xcom_pull(key="node_memory_today", task_ids="node_memory_today") or "No data"
    node_disk = ti.xcom_pull(key="node_disk_today", task_ids="node_disk_today") or "No data"
    node_readiness = ti.xcom_pull(key="node_readiness_check", task_ids="node_readiness_check") or "No data"
    
    # Pods & K8s
    pod_today = ti.xcom_pull(key="pod_details_today", task_ids="pod_details_today") or "No pod data"
    pod_restart = ti.xcom_pull(key="pod_restart_today", task_ids="pod_restart_today") or "No data"
    
    k8s_eol = ti.xcom_pull(key="kubernetes_eol_and_next_version", task_ids="kubernetes_eol_and_next_version") or "No data"
    microk8s_exp = ti.xcom_pull(key="microk8s_expiry_check", task_ids="microk8s_expiry_check") or "No data"

    # --- Part 2: Comparisons ---
    node_cpu_cmp = ti.xcom_pull(key="node_cpu_today_vs_yesterday", task_ids="node_cpu_today_vs_yesterday") or "No comparison data"
    node_mem_cmp = ti.xcom_pull(key="node_memory_today_vs_yesterday", task_ids="node_memory_today_vs_yesterday") or "No comparison data"
    node_disk_cmp = ti.xcom_pull(key="node_disk_today_vs_yesterday", task_ids="node_disk_today_vs_yesterday") or "No comparison data"
    
    pod_cmp = ti.xcom_pull(key="pod_details_today_vs_yesterday", task_ids="pod_details_today_vs_yesterday") or "No comparison data"
                        
    overall_summary = ti.xcom_pull(key="overall_summary", task_ids="overall_summary") or "No summary"

    # 3. Building Report
    report = f"""
# SRE Daily Report – MBK Platform
**Generated**: **11:00 AM IST**

---

## 1. Node-Level Metrics (Last 24h)
{node_cpu}
{node_memory}
{node_disk}
{node_readiness}

---

## 2. Pod-Level Metrics (Last 24h) – **Grouped by Namespace**
{pod_today}

---

## 3. Pod Restart Count (Last 24h)
{pod_restart}

---

## 4. Kubernetes Check
{k8s_eol}
{microk8s_exp}

---

## 5. Node-Level Metrics (Today vs Yesterday)
{node_cpu_cmp}
{node_mem_cmp}
{node_disk_cmp}

---

## 6. Pod-Level CPU & Memory (Today vs Yesterday)
{pod_cmp}

---

## 7. Overall Summary
{overall_summary}

---

**End of Report** *Generated by MBK Agent @ 11:00 AM IST*
""".strip()

    report = re.sub(r'\n{3,}', '\n\n', report)
    ti.xcom_push(key="sre_full_report", value=report)
    logging.info("SRE report compiled successfully.")
    return report


# === t11: Convert SRE Markdown Report to HTML (Local Markdown Parser) ===
def preprocess_markdown(markdown_text):
    """
    Clean and standardize Markdown before conversion to HTML.
    Fixes spacing and table formatting issues.
    """
    markdown_text = markdown_text.lstrip('\ufeff\u200b\u200c\u200d')
    markdown_text = re.sub(r'^(#{1,6})\s*', r'\1 ', markdown_text, flags=re.MULTILINE)
    
    lines = markdown_text.split('\n')
    processed = []
    in_table = False

    for line in lines:
        stripped = line.strip()
        if '|' in stripped and stripped.count('|') >= 2:
            if not in_table and processed and processed[-1].strip():
                processed.append('')
            in_table = True
            processed.append(line)
        else:
            if in_table and stripped:
                processed.append('')
                in_table = False
            processed.append(line)
    return '\n'.join(processed)


def convert_to_html(ti, **context):
    """
    Airflow task: Convert the Markdown SRE report into a clean, responsive, Gmail-safe HTML.
    """
    # --- Pull Markdown from previous task ---
    markdown_report = ti.xcom_pull(key="sre_full_report", task_ids="compile_sre_report") or "# No report generated."
    logging.info(f"Markdown length: {len(markdown_report)}")

    # --- Preprocess Markdown ---
    markdown_report = preprocess_markdown(markdown_report)

    html_body = None

    # --- Try python-markdown (best support for tables & lists) ---
    try:
        import markdown
        logging.info("✅ Using 'markdown' library for conversion")
        html_body = markdown.markdown(
            markdown_report,
            extensions=[
                'tables',
                'fenced_code',
                'nl2br',
                'sane_lists',
                'attr_list'
            ]
        )
    except ImportError:
        logging.warning("⚠️ 'markdown' library not found.")
    except Exception as e:
        logging.error(f"⚠️ markdown library error: {e}")

    # --- Fallback to markdown2 ---
    if not html_body or len(html_body) < 100:
        try:
            import markdown2
            logging.info("✅ Using 'markdown2' fallback parser")
            html_body = markdown2.markdown(
                markdown_report,
                extras=[
                    "fenced-code-blocks",
                    "tables",
                    "strike",
                    "task_list",
                    "cuddled-lists",
                    "header-ids",
                    "footnotes",
                    "break-on-newline"
                ]
            )
        except ImportError:
            logging.error("❌ markdown2 library not installed (pip install markdown2)")
        except Exception as e:
            logging.error(f"❌ markdown2 error: {e}")

    # --- Fallback to mistune (final safety) ---
    if not html_body or len(html_body) < 50:
        try:
            import mistune
            logging.info("✅ Using 'mistune' parser as final fallback")
            html_body = mistune.html(markdown_report)
        except Exception as e:
            logging.error(f"❌ Mistune conversion failed: {e}")
            html_body = f"<pre>{html.escape(markdown_report)}</pre>"

    # --- Wrap HTML with email-safe container ---
    full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SRE Daily Report</title>
<style>
body {{
    font-family: Arial, sans-serif;
    background-color: #f4f4f4;
    margin: 0;
    padding: 20px;
}}
.container {{
    max-width: 1000px;
    background: #ffffff;
    margin: auto;
    padding: 30px;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}}
table {{
    border-collapse: collapse;
    width: 100%;
    margin: 20px 0;
    font-size: 14px;
}}
table, th, td {{
    border: 1px solid #ddd;
}}
th, td {{
    padding: 10px;
    text-align: left;
}}
th {{
    background-color: #1a5fb4;
    color: #ffffff;
}}
tr:nth-child(even) {{
    background-color: #f8f9fa;
}}
pre {{
    background-color: #f6f8fa;
    padding: 12px;
    border-radius: 6px;
    overflow-x: auto;
    border: 1px solid #e1e4e8;
}}
h1 {{
    color: #1a5fb4;
    font-size: 24px;
    border-bottom: 3px solid #1a5fb4;
    padding-bottom: 6px;
}}
h2 {{
    color: #1a5fb4;
    border-bottom: 2px solid #ccc;
    padding-bottom: 4px;
}}
h3 {{
    color: #2d3748;
}}
strong {{
    color: #111;
}}
ul, ol {{
    padding-left: 25px;
}}
a {{
    color: #1a5fb4;
    text-decoration: none;
}}
a:hover {{
    text-decoration: underline;
}}
@media screen and (max-width: 600px) {{
    .container {{
        padding: 15px;
    }}
    table {{
        font-size: 12px;
    }}
}}
</style>
</head>
<body>
<div class="container">
{html_body}
</div>
</body>
</html>"""

    # --- Log preview ---
    logging.info(f"HTML generated, length={len(full_html)}")

    # --- Push to XCom for next task (email sender) ---
    ti.xcom_push(key="sre_html_report", value=full_html)

    logging.info("✅ SRE Markdown successfully converted to HTML.")
    return full_html


# === t12: Send SRE Report via Gmail ===
def send_sre_email(ti, **context):
    html_report = ti.xcom_pull(key="sre_html_report", task_ids="convert_to_html")
    if not html_report or "<html" not in html_report.lower():
        logging.error("No valid HTML report found in XCom.")
        raise ValueError("HTML report missing or invalid.")
    
    pdf_path = ti.xcom_pull(key="sre_pdf_path", task_ids="generate_pdf")
    pdf_attachment = None
    if pdf_path and os.path.exists(pdf_path):
        with open(pdf_path, "rb") as f:
            pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
            pdf_attachment.add_header(
                "Content-Disposition",
                "attachment",
                filename=f"MBK_SRE_Report.pdf"
            )
        logging.info(f"Attaching PDF: {pdf_path}")
    else:
        logging.warning("PDF not found or not generated, skipping attachment")

    # Clean up any code block wrappers
    html_body = re.sub(r'```html\s*|```', '', html_report).strip()

    subject = f"SRE Daily Report – {datetime.utcnow().strftime('%Y-%m-%d')}"
    recipient = RECEIVER_EMAIL
    try:
        # Initialize SMTP server
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=50)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        # Create MIME message
        logging.debug(f"Preparing email to {recipient} with subject: {subject}")
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = f"MBK SRE Agent {SMTP_SUFFIX}"
        msg["To"] = recipient

        # --- HTML Part (with possible inline images) ---
        html_part = MIMEMultipart("related")
        html_part.attach(MIMEText(html_body, "html"))

        # Optional: Inline chart image (unchanged from your original)
        chart_b64 = ti.xcom_pull(key="chart_b64", task_ids="generate_sre_charts")
        if chart_b64:
            try:
                img_data = base64.b64decode(chart_b64)
                img_part = MIMEImage(img_data, 'png')
                img_part.add_header('Content-ID', '<chart_image>')
                img_part.add_header('Content-Disposition', 'inline', filename='chart.png')
                html_part.attach(img_part)
                logging.info("Attached chart image to email.")
            except Exception as e:
                logging.warning(f"Failed to attach inline image: {str(e)}")

        # Attach the HTML+inline part to the main message
        msg.attach(html_part)

        # --- PDF Attachment ---
        if pdf_attachment:
            msg.attach(pdf_attachment)
            logging.info("PDF successfully attached to email")
        else:
            logging.info("No PDF attachment added")

        # Send the email
        server.sendmail("webmaster@ecloudcontrol.com", recipient, msg.as_string())
        logging.info(f"Email sent successfully: {recipient}")
        server.quit()
        return True
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        return None

def generate_pdf_report_callable(ti=None, **context):
    """
    MBK SRE PDF – ReportLab (ABSOLUTE FINAL VERSION)
    • No raw # or ## visible
    • Tables perfect with text wrap
    • No small squares
    • Professional layout
    """
    try:
        md = ti.xcom_pull(key="sre_full_report", task_ids="compile_sre_report") or "# No report generated."
        md = preprocess_markdown(md)

        date_str = YESTERDAY_DATE_STR
        out_path = f"/tmp/MBK_SRE_Report_{date_str}.pdf"

        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_LEFT, TA_CENTER
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Preformatted, PageBreak

        doc = SimpleDocTemplate(
            out_path,
            pagesize=A4,
            leftMargin=28, rightMargin=28,
            topMargin=30, bottomMargin=30
        )

        styles = getSampleStyleSheet()
        title = ParagraphStyle('Title', parent=styles['Title'], fontSize=22, leading=28, alignment=TA_CENTER,
                               spaceAfter=20, textColor=colors.HexColor("#1a5fb4"), fontName="Helvetica-Bold")
        h2 = ParagraphStyle('H2', parent=styles['Heading2'], fontSize=15, leading=20, spaceBefore=18, spaceAfter=10,
                            textColor=colors.HexColor("#1a5fb4"), fontName="Helvetica-Bold")
        h3 = ParagraphStyle('H3', parent=styles['Heading3'], fontSize=12, leading=16, spaceBefore=12, spaceAfter=8)
        normal = ParagraphStyle('Normal', parent=styles['Normal'], fontSize=10, leading=14, spaceAfter=8)
        small = ParagraphStyle('Small', parent=styles['Normal'], fontSize=9, textColor=colors.gray, spaceAfter=12)
        code = ParagraphStyle('Code', fontName='Courier', fontSize=8, leading=10,
                              backColor=colors.HexColor("#f6f8fa"), borderPadding=10,
                              borderColor=colors.lightgrey, borderWidth=1, borderRadius=4)
        cell_style = ParagraphStyle('Cell', parent=styles['Normal'], fontSize=8.5, leading=10, alignment=TA_LEFT)

        flowables = []

        lines = md.splitlines()
        i = 0
        in_code = False
        code_lines = []

        while i < len(lines):
            raw_line = lines[i]
            stripped = raw_line.strip()

            # === CODE BLOCKS ===
            if stripped.startswith("```"):
                if in_code:
                    flowables.append(Preformatted("\n".join(code_lines), code))
                    flowables.append(Spacer(1, 10))
                    code_lines = []
                in_code = not in_code
                i += 1
                continue
            if in_code:
                code_lines.append(raw_line)
                i += 1
                continue

            # === SKIP HR LINES (no squares) ===
            if stripped.startswith(("---", "***", "___")):
                flowables.append(Spacer(1, 14))
                i += 1
                continue

            # === HEADINGS – NOW WORKS EVEN WITH LEADING SPACES ===
            if raw_line.lstrip().startswith("# "):
                text = raw_line.lstrip("# ").strip()
                flowables.append(Paragraph(text, title))
                flowables.append(Spacer(1, 14))
            elif raw_line.lstrip().startswith("## "):
                text = raw_line.lstrip("# ").strip()
                flowables.append(Paragraph(text, h2))
                flowables.append(Spacer(1, 10))
            elif raw_line.lstrip().startswith("### "):
                text = raw_line.lstrip("# ").strip()
                flowables.append(Paragraph(text, h3))
                flowables.append(Spacer(1, 8))
            elif raw_line.lstrip().startswith("#### "):
                text = raw_line.lstrip("# ").strip()
                flowables.append(Paragraph(f"<b>{text}</b>", normal))

            # === TABLES – PERFECT TEXT WRAP ===
            elif "|" in raw_line and i + 1 < len(lines) and re.match(r"^[\s\|:-]*$", lines[i+1].strip()):
                table_data = []
                header = [c.strip() for c in raw_line.split("|")[1:-1]]
                table_data.append(header)
                i += 2
                while i < len(lines) and "|" in lines[i]:
                    row = [c.strip() for c in lines[i].split("|")[1:-1]]
                    if row:
                        table_data.append(row)
                    i += 1

                if len(table_data) > 1:
                    num_cols = len(table_data[0])
                    col_width = doc.width / num_cols
                    wrapped_data = [[Paragraph(html.escape(cell), cell_style) for cell in row] for row in table_data]

                    table = Table(wrapped_data, colWidths=[col_width] * num_cols, repeatRows=1)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#1a5fb4")),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, -1), 8.5),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor("#f9f9f9")),
                        ('LEFTPADDING', (0, 0), (-1, -1), 6),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                        ('TOPPADDING', (0, 0), (-1, -1), 8),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ]))
                    flowables.append(table)
                    flowables.append(Spacer(1, 14))
                continue

            # === NORMAL TEXT ===
            elif stripped:
                text = html.escape(raw_line)
                text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
                text = re.sub(r'__(.*?)__', r'<b>\1</b>', text)
                text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)
                text = re.sub(r'`([^`]+)`', r'<font name=Courier>\1</font>', text)
                flowables.append(Paragraph(text, normal))
            else:
                flowables.append(Spacer(1, 6))

            i += 1

        # Footer
        flowables.append(PageBreak())
        flowables.append(Paragraph("End of Report", h2))
        flowables.append(Paragraph("Generated by MBK SRE Agent • Powered by Airflow + ReportLab", small))

        doc.build(flowables)
        logging.info(f"PDF generated – PERFECT (no # visible, tables wrapped): {out_path}")
        ti.xcom_push(key="sre_pdf_path", value=out_path)
        return out_path

    except Exception as e:
        logging.error("PDF generation failed", exc_info=True)
        raise

# === DAG ===
with DAG(
    dag_id="sre-mbk_daily",
    default_args=default_args,
    schedule="30 5 * * *",  # 05:30 UTC = 11:00 AM IST
    catchup=False,
    tags=["sre", "mbk", "daily", "11am-ist"],
    max_active_runs=1,
) as dag:

    # Static Tasks - Today
    t1 = PythonOperator(task_id="node_cpu_today", python_callable=node_cpu_today)
    t2 = PythonOperator(task_id="node_memory_today", python_callable=node_memory_today)
    t3 = PythonOperator(task_id="node_disk_today", python_callable=node_disk_today)
    t4 = PythonOperator(task_id="node_readiness_check", python_callable=fetch_node_readiness)
    t5 = PythonOperator(task_id="pod_restart_today", python_callable=pod_restart_today)
    t6 = PythonOperator(task_id="ssl_cert_expiry_check", python_callable=check_ssl_cert_expiry)
    t7 = PythonOperator(task_id="kubernetes_eol_and_next_version", python_callable=kubernetes_eol_and_next_version)
    t8 = PythonOperator(task_id="microk8s_expiry_check", python_callable=fetch_microk8s_expiry)

    # Static Tasks - Yesterday (previous period)
    t10 = PythonOperator(task_id="node_cpu_yesterday", python_callable=node_cpu_yesterday)
    t11 = PythonOperator(task_id="node_memory_yesterday", python_callable=node_memory_yesterday)
    t12 = PythonOperator(task_id="node_disk_yesterday", python_callable=node_disk_yesterday)

    # Comparison tasks
    t15 = PythonOperator(task_id="node_cpu_today_vs_yesterday", python_callable=node_cpu_today_vs_yesterday)
    t16 = PythonOperator(task_id="node_memory_today_vs_yesterday", python_callable=node_memory_today_vs_yesterday)
    t17 = PythonOperator(task_id="node_disk_today_vs_yesterday", python_callable=node_disk_today_vs_yesterday)

    # Pod Tasks - Today
    t_pod_today = PythonOperator(
        task_id="pod_details_today",
        python_callable=pod_details_today        
    )

    # Pod Tasks - Yesterday
    t_pod_yesterday = PythonOperator(
        task_id="pod_details_yesterday",
        python_callable=pod_details_yesterday        
    )

    # Pod Tasks - Comparison
    t_pod_comparison = PythonOperator(
        task_id="pod_details_today_vs_yesterday",
        python_callable=pod_details_today_vs_yesterday        
    )

    # Summary Tasks
    t18_1 = PythonOperator(task_id="summarize_node_cpu", python_callable=summarize_node_cpu)
    t18_2 = PythonOperator(task_id="summarize_node_memory", python_callable=summarize_node_memory)
    t18_3 = PythonOperator(task_id="summarize_node_disk", python_callable=summarize_node_disk)
    t18_4 = PythonOperator(task_id="summarize_pod_restarts", python_callable=summarize_pod_restarts)
    # t18_5 = PythonOperator(task_id="summarize_pod_metrics", python_callable=summarize_pod_metrics)
    t18_6 = PythonOperator(task_id="summarize_health_checks", python_callable=summarize_health_checks)

    # Final tasks
    t20 = PythonOperator(task_id="overall_summary", python_callable=overall_summary)
    t21 = PythonOperator(task_id="compile_sre_report", python_callable=compile_sre_report)
    t_generate_pdf = PythonOperator(task_id="generate_pdf", python_callable=generate_pdf_report_callable)
    t22 = PythonOperator(task_id="convert_to_html", python_callable=convert_to_html)
    t23 = PythonOperator(task_id="send_sre_email", python_callable=send_sre_email)

    # === DEPENDENCIES ===

    # 1. Comparison Tasks (Depend on Today + Yesterday)
    [t1, t10] >> t15   # CPU
    [t2, t11] >> t16   # Memory
    [t3, t12] >> t17   # Disk
    
    # 2. Pod Comparison (Depends on Pod Today + Pod Yesterday)
    [t_pod_today, t_pod_yesterday] >> t_pod_comparison

    # 3. Overall Summary (t20)
    # This must wait for ALL data generation and comparison tasks to finish.
    # We include:
    # - Static checks (t4, t5, t7, t7_1, t8)
    # - Comparison results (t15, t16, t17, t18, t19)
    # - Pod results (t_pod_today, t_pod_comparison)
    
    summary_dependencies = [
        t4, t5, t6, t7, t8,         # Single metrics (Readiness, Pod Restart, K8s versions, Certs)
        t15, t16, t17,       # Infrastructure Comparisons
        t_pod_today, t_pod_comparison # Pod Metrics
    ]

    summary_dependencies >> t18_1 >> t18_2 >> t18_3 >> t18_4 >> t18_6 >> t20

    # 4. Final Reporting Pipeline
    t20 >> t21 >> t_generate_pdf >> t22 >> t23
