from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum

# Set IST timezone
ist = pendulum.timezone("Asia/Kolkata")

# Fetch Airflow variables
api_token = Variable.get("API_TOKEN")
api_url = Variable.get("API_URL")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 12, tzinfo=ist),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='WebshopChatAPIAutomation',
    default_args=default_args,
    schedule_interval='30 14 * * *',  # 14:30 IST
    catchup=False,
    tags=['maven', 'automation', 'test'],
) as dag:

    run_mvn_test = BashOperator(
        task_id='run_mvn_test',
        bash_command=(
            f'cd /appz/home/airflow/dags/agent_dags/WebshopChatAPIAutomation && '
            f'API_TOKEN="{api_token}" API_URL="{api_url}" mvn test'
        )
    )

    run_mvn_test
