from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Simple function to print a message


# DAG definition
with DAG(
    'minute_runner',
    default_args=default_args,
    description='A simple DAG that runs every minute',
    schedule_interval='* * * * *',  # Cron expression for every minute
    catchup=False,
) as dag:
    def say_hello():
        print("Hello! This DAG runs every minute.")

    # PythonOperator to run the simple function
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
        provide_context=True,
    )
    hello_task