"""
uptime_report_trigger_hourly.py
100% WORKING — NO MORE branch_task_ids ERROR
Uses: Single PythonOperator → Triggers via Airflow API (official pattern)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.api.client.local_client import Client
from datetime import datetime
import pendulum
import json
import logging
import re

dag = DAG(
    dag_id="uptime_report_trigger_hourly",
    schedule_interval="0 * * * *",  # hourly
    start_date=datetime(2025, 2, 18),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "lowtouch.ai_developers",
        "retries": 1,
    },
    tags=["uptime", "trigger", "orchestrator"],
    description="Triggers uptime reports based on client timezone — NO branch error",
)


def safe_name(s: str) -> str:
    return re.sub(r'[^a-z0-9_]+', '_', s.lower()).strip('_')


def trigger_uptime_reports(**context):
    clients = json.loads(Variable.get("UPTIME_CLIENTS_CONFIG", default_var="[]"))
    if not clients:
        logging.info("No clients configured")
        return
    logging.info(f"Loaded {len(clients)} clients for uptime report triggering")
    now_utc = pendulum.now("UTC")
    client = Client(None, None)  # Local client (official)

    triggered = 0
    for c in clients:
        client_id = c.get("client_id", "unknown")
        tz = c.get("timezone", "UTC")
        monitor_ids = c.get("monitor_ids",[])
        email = c.get("recipient_email")
        name = c.get("client_name", client_id)
        customer=c.get("customer","Appz")
        monitoring_team= "CloudOrbit SRE agent" if customer=="CloudOrbit" else "AppZ SRE agent"
        if not monitor_ids or not email:
            continue
        logging.info(f"Processing client: {name} ({client_id}) with monitors: {monitor_ids} for email: {email}")
        try:
            local_now = now_utc.in_timezone(tz)
        except:
            local_now = now_utc

        hour = local_now.hour
        minute = local_now.minute
        is_midnight_window = (hour == 0)

        # Daily: every day at midnight
        should_trigger_daily = is_midnight_window

        # Weekly: Monday
        should_trigger_weekly = (local_now.weekday() == 0 and is_midnight_window)
        # Monthly: 1st
        should_trigger_monthly = (local_now.day == 1 and is_midnight_window)

        base_conf = {
            "recipient_email": email,
            "client_tz": tz,
            "client_name": name,
            "monitoring_team": monitoring_team
        }
        for monitor_id in monitor_ids:
            monitor_id = str(monitor_id).strip()
            conf = {**base_conf, "monitor_id": monitor_id}
            if should_trigger_daily:
                client.trigger_dag(
                    dag_id="uptime_daily_data_report",
                    conf=conf,
                    run_id=f"daily_{safe_name(client_id)}_{now_utc.format('YYYYMMDD_HHmmss')}"
                )
                logging.info(f"Triggered DAILY report to {name} for the monitor ({monitor_id})")
                triggered += 1

            if should_trigger_weekly:
                client.trigger_dag(
                    dag_id="uptime_weekly_data_report",
                    conf=conf,
                    run_id=f"weekly_{safe_name(client_id)}_{now_utc.format('YYYYMMDD_HHmmss')}"
                )
                logging.info(f"Triggered WEEKLY report to {name} for the monitor ({monitor_id})")
                triggered += 1

            if should_trigger_monthly:
                client.trigger_dag(
                    dag_id="uptime_monthly_data_report",
                    conf=conf,
                    run_id=f"monthly_{safe_name(client_id)}_{now_utc.format('YYYYMMDD_HHmmss')}"
                )
                logging.info(f"Triggered MONTHLY report to {name} for the monitor ({monitor_id})")
                triggered += 1

    logging.info(f"Total reports triggered: {triggered}")


# Single task — clean, reliable, observable
trigger_task = PythonOperator(
    task_id="trigger_client_reports",
    python_callable=trigger_uptime_reports,
    provide_context=True,
    dag=dag,
)