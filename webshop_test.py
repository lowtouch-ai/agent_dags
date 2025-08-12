from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# Set IST timezone
ist = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 12, tzinfo=ist),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='mvn_test_webshopchatapi',
    default_args=default_args,
    schedule_interval='0 9 * * *',  # 09:00 UTC = 14:30 IST
    catchup=False,
    tags=['maven', 'automation', 'test'],
) as dag:

    run_mvn_test = BashOperator(
        task_id='run_mvn_test',
        bash_command='cd /appz/home/airflow/dags/agent_dags/WebshopChatAPIAutomation && mvn test'
    )

    run_mvn_test
