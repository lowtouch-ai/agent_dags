import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def print_env_vars():
    print("🌍 Environment Variables:")
    for key, value in os.environ.items():
        print(f"{key}={value}")

def check_postgres_auth_wrong():
    host = "postgres"
    database = "postgres"
    user = os.getenv("AIRFLOW_POSTGRES_USER")
    password = os.getenv("VAULT_SECRET_ID")  # wrong env
    conn = psycopg2.connect(
        host=host,
        dbname=database,
        user=user,
        password=password
    )

def check_postgres_auth_correct():
    host = "postgres"
    database = "postgres"
    user = os.getenv("AIRFLOW_POSTGRES_USER")
    password = os.getenv("AIRFLOW_POSTGRES_PASSWORD")  # correct env
    conn = psycopg2.connect(
        host=host,
        dbname=database,
        user=user,
        password=password
    )

def print_message():
    print("🧩 Python operator executed successfully!")

with DAG(
    dag_id="airflow_log_simulate",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "auth"],
) as dag:

    print_env = PythonOperator(
        task_id="environment_variables",
        python_callable=print_env_vars,
    )

    fail_auth = PythonOperator(
        task_id="failed_authentication",
        python_callable=check_postgres_auth_wrong,
        trigger_rule='all_done',
    )

    success_auth = PythonOperator(
        task_id="success_authentication",
        python_callable=check_postgres_auth_correct,
        trigger_rule='all_done',
    )

    print_task = PythonOperator(
        task_id="python_operator",
        python_callable=print_message,
        trigger_rule='all_done',
    )

    bash_task = BashOperator(
        task_id="bash_operator",
        bash_command="echo Date: $(date)",
        trigger_rule='all_done',
    )

    print_env >> fail_auth >> success_auth >> print_task >> bash_task
