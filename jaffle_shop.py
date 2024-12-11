from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pathlib import Path
import os

def on_failure_callback(context,SVC_NAME):
    svc=SVC_NAME
    task=context.get("task_instance").task_id
    dag=context.get("task_instance").dag_id
    ti=context.get("task_instance")
    exec_date=context.get("execution_date")
    dag_run = context.get('dag_run')
    log_url = context.get("task_instance").log_url
    msg = f""" 
            SVC: {svc}
            Dag: {dag}
            Task: {task}
            DagRun: {dag_run}
            TaskInstance: {ti}
            Log Url: {log_url} 
            Execution Time: {exec_date} 
            """
    print(msg)

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath = "/appz/home/airflow/dags/dbt/jaffle_shop/profiles.yml",
)

def print_variable(**kwargs):
  variable = kwargs['dag_run'].conf.get('payment_type')
  print(variable)

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'README.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

with DAG(
    dag_id="jaffle_shop",
    start_date=datetime(2023, 11, 10),
    schedule='0 0/12 * * *',
    tags=["sample-dag"],
    doc_md=readme_content,
    default_args = {
    "owner": "airflow"
    },
    catchup=False,
):
    e1 = PythonOperator(task_id = "print_variables",
                        python_callable = print_variable,
                        provide_context=True,
                       )

    seeds_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        select=["path:seeds/"],
    ),
        default_args={"retries": 2},
        group_id = "dbt_seeds_group"
    )

    stg_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        select=["path:models/staging/"],
    ),
        default_args={"retries": 1,
                     'on_failure_callback': lambda context: on_failure_callback(context,"TEST_SVC_NAME"),},
        group_id = "dbt_stg_group"
    )

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
    ),
        operator_args={
            "append_env": True,
           # "queue": "on-premises",
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        exclude=["path:models/staging","path:seeds/"],
    ),
        default_args={"retries": 2},
    )
    
    # send_email = EmailOperator( 
    #     task_id='send_email', 
    #     to='mpmathew@ecloudcontrol.com', 
    #     subject='test email for airflow', 
    #     html_content="Date: {{ ds }}", 
    # )
   
    e2 = EmptyOperator(task_id="post_dbt")
    
e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2
