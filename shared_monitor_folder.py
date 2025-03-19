from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Simple function to print a message
def say_hello():
    print("Hello! This DAG runs every minute.")

# DAG definition
with DAG(
    'minute_runner',
    default_args=default_args,
    description='A simple DAG that runs every minute',
    schedule_interval='* * * * *',  # Cron expression for every minute
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # PythonOperator to run the simple function
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )