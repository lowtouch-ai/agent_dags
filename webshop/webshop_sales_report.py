from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests, json, logging

logger = logging.getLogger(__name__)

default_args = {"owner": "webshop", "start_date": datetime(2025, 1, 1), "retries": 1}

def generate_sales_report(**context):
    conf = context["dag_run"].conf or {}
    start_date = conf.get("start_date", "2018-01-01")
    end_date = conf.get("end_date", "2018-12-31")

    logger.info(f"Generating sales report for {start_date} to {end_date}")

    # Call webshop analytics API
    sales = requests.get(
        "http://agentconnector:8000/webshop/analytics/sales",
        params={"aggregation": "monthly", "start_date": start_date, "end_date": end_date}
    ).json()

    top = requests.get(
        "http://agentconnector:8000/webshop/product/top-selling/",
        params={"start_date": start_date, "end_date": end_date}
    ).json()

    report = {"period": f"{start_date} to {end_date}", "sales_data": sales, "top_selling": top}
    context["ti"].xcom_push(key="report", value=json.dumps(report))
    logger.info(f"Sales report generated successfully for {start_date} to {end_date}")
    return report

with DAG(
    "webshop_sales_report",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["webshop", "sales", "report"],
    description="Generates the webshop sales performance report for a given date range. Returns a run ID to track progress.",
    params={
        "start_date": Param(
            type="string",
            description="Report start date (YYYY-MM-DD format, e.g. 2018-01-01)",
        ),
        "end_date": Param(
            type="string",
            description="Report end date (YYYY-MM-DD format, e.g. 2018-12-31)",
        ),
    },
) as dag:
    PythonOperator(task_id="generate_report", python_callable=generate_sales_report)
