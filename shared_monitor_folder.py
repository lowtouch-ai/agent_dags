from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# Define the DAG
with DAG(
    dag_id='print_date_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Task to print the date
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    print_date
#testing