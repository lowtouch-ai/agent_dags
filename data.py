from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.models import Variable
import os

default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dbt_project_dir = "/appz/home/airflow/dags/agent_dags/dbt/webshop"
dbt_executable_path = "/dbt_venv/bin/dbt"
dbt_venv_path = "/dbt_venv/bin/activate"

postgres_user = Variable.get("WEBSHOP_POSTGRES_USER")
postgres_password = Variable.get("WEBSHOP_POSTGRES_PASSWORD")

dbt_seed_commands = [
    "address", "articles", "colors", "customer", "labels",
    "order_positions", "order_seed", "products", "stock", "sizes"
]

dbt_run_commands = ["order"]

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'webshop.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

daily_schedule_utc = "30 2 * * *"

with DAG(
    'webshop_reset_data',
    default_args=default_args,
    schedule_interval=daily_schedule_utc,
    catchup=False,
    tags=["reset", "webshop", "data"]
) as dag:

    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"source {dbt_venv_path} && "
            f"cd {dbt_project_dir} && "
            f"{dbt_executable_path} deps"
        ),
        env={
            "WEBSHOP_POSTGRES_USER": postgres_user,
            "WEBSHOP_POSTGRES_PASSWORD": postgres_password,
            "DBT_PROFILES_DIR": dbt_project_dir,
            "ELEMENTARY_ORCHESTRATOR": "airflow",
            "ELEMENTARY_JOB_NAME": "webshop_reset_data_elementary",
            "ELEMENTARY_JOB_ID": "{{ ti.dag_id }}",
            "ELEMENTARY_JOB_RUN_ID": "{{ ti.run_id }}"
        }
    )

    with TaskGroup("dbt_seed") as dbt_seed_group:
        for seed in dbt_seed_commands:
            BashOperator(
                task_id=f"dbt_seed_{seed}",
                bash_command=(
                    f"source {dbt_venv_path} && "
                    f"cd {dbt_project_dir} && "
                    f"{dbt_executable_path} seed --select {seed} "
                    f"--vars '{{\\\"orchestrator\\\": \\\"airflow\\\", "
                    f"\\\"job_name\\\": \\\"webshop_reset_data_elementary\\\", "
                    f"\\\"job_id\\\": \\\"{{{{ ti.dag_id }}}}\\\", "
                    f"\\\"job_run_id\\\": \\\"{{{{ ti.run_id }}}}\\\"}}'"
                ),
                env={
                    "WEBSHOP_POSTGRES_USER": postgres_user,
                    "WEBSHOP_POSTGRES_PASSWORD": postgres_password,
                    "DBT_PROFILES_DIR": dbt_project_dir,
                    "ELEMENTARY_ORCHESTRATOR": "airflow",
                    "ELEMENTARY_JOB_NAME": "webshop_reset_data_elementary",
                    "ELEMENTARY_JOB_ID": "{{ ti.dag_id }}",
                    "ELEMENTARY_JOB_RUN_ID": "{{ ti.run_id }}"
                }
            )

    with TaskGroup("dbt_run") as dbt_run_group:
        for run in dbt_run_commands:
            BashOperator(
                task_id=f"dbt_run_{run}",
                bash_command=(
                    f"source {dbt_venv_path} && "
                    f"cd {dbt_project_dir} && "
                    f"{dbt_executable_path} run --select {run} "
                    f"--vars '{{\\\"orchestrator\\\": \\\"airflow\\\", "
                    f"\\\"job_name\\\": \\\"webshop_reset_data_elementary\\\", "
                    f"\\\"job_id\\\": \\\"{{{{ ti.dag_id }}}}\\\", "
                    f"\\\"job_run_id\\\": \\\"{{{{ ti.run_id }}}}\\\"}}'"
                ),
                env={
                    "WEBSHOP_POSTGRES_USER": postgres_user,
                    "WEBSHOP_POSTGRES_PASSWORD": postgres_password,
                    "DBT_PROFILES_DIR": dbt_project_dir,
                    "ELEMENTARY_ORCHESTRATOR": "airflow",
                    "ELEMENTARY_JOB_NAME": "webshop_reset_data_elementary",
                    "ELEMENTARY_JOB_ID": "{{ ti.dag_id }}",
                    "ELEMENTARY_JOB_RUN_ID": "{{ ti.run_id }}"
                }
            )

    dbt_run_elementary = BashOperator(
        task_id="dbt_run_elementary",
        bash_command=(
            f"source {dbt_venv_path} && "
            f"cd {dbt_project_dir} && "
            f"{dbt_executable_path} run --select elementary "
            f"--vars '{{\\\"orchestrator\\\": \\\"airflow\\\", "
            f"\\\"job_name\\\": \\\"webshop_reset_data_elementary\\\", "
            f"\\\"job_id\\\": \\\"{{{{ ti.dag_id }}}}\\\", "
            f"\\\"job_run_id\\\": \\\"{{{{ ti.run_id }}}}\\\"}}'"
        ),
        env={
            "WEBSHOP_POSTGRES_USER": postgres_user,
            "WEBSHOP_POSTGRES_PASSWORD": postgres_password,
            "DBT_PROFILES_DIR": dbt_project_dir,
            "ELEMENTARY_ORCHESTRATOR": "airflow",
            "ELEMENTARY_JOB_NAME": "webshop_reset_data_elementary",
            "ELEMENTARY_JOB_ID": "{{ ti.dag_id }}",
            "ELEMENTARY_JOB_RUN_ID": "{{ ti.run_id }}"
        }
    )

    elementary_report_task = BashOperator(
        task_id="generate_elementary_report",
        bash_command=(
            f"mkdir -p {dbt_project_dir}/edr_target && "
            f"source {dbt_venv_path} && "
            f"/dbt_venv/bin/edr report "
            f"--project-dir {dbt_project_dir} "
            f"--profiles-dir {dbt_project_dir} "
            f"--target-path {dbt_project_dir}/edr_target"
        ),
        env={
            "WEBSHOP_POSTGRES_USER": postgres_user,
            "WEBSHOP_POSTGRES_PASSWORD": postgres_password,
            "DBT_PROFILES_DIR": dbt_project_dir,
            "ELEMENTARY_ORCHESTRATOR": "airflow",
            "ELEMENTARY_JOB_NAME": "webshop_reset_data_elementary",
            "ELEMENTARY_JOB_ID": "{{ ti.dag_id }}",
            "ELEMENTARY_JOB_RUN_ID": "{{ ti.run_id }}"
        }
    )

    copy_elementary_report = BashOperator(
        task_id="copy_elementary_report",
        bash_command=(
            f"rm -rf /appz/home/airflow/docs/edr_target && "
            f"cp -r {dbt_project_dir}/edr_target /appz/home/airflow/docs/"
        )
    )

    # DAG dependencies
    dbt_deps_task >> dbt_seed_group >> dbt_run_group >> dbt_run_elementary >> elementary_report_task >> copy_elementary_report
