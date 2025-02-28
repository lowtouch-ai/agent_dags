from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable, DagRun, TaskInstance
from airflow.utils.session import provide_session
from datetime import datetime, timedelta
import json
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default args for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# API Endpoint and Credentials
AUTOLOAN_API_URL = Variable.get("AUTOLOAN_API_URL")
AGENTOMATIC_API_URL = Variable.get("AGENTOMATIC_API_URL")
TEST_PHONE_NUMBER = Variable.get("TEST_PHONE_NUMBER")

# Ensure API URL is set
if not AUTOLOAN_API_URL:
    raise ValueError("Autoloan API URL is missing. Set it in Airflow Variables.")

# Custom ExternalTaskSensor with XCom-based validation
class XComExternalTaskSensor(ExternalTaskSensor):
    def poke(self, context):
        """Custom poke method to check XCom value instead of execution_date."""
        ti = context['ti']
        trigger_run_id = ti.xcom_pull(task_ids='trigger_twilio_voice_call', key='triggered_run_id')
        
        if not trigger_run_id:
            logger.warning("No triggered_run_id found, continuing to wait...")
            return False

        # Fetch the XCom value from the external task
        external_task_xcom = ti.xcom_pull(
            dag_id=self.external_dag_id,
            task_ids=self.external_task_id,
            key='recording_status',
            run_id=trigger_run_id  # Use run_id instead of execution_date
        )

        logger.info(f"Checking XCom recording_status for run_id {trigger_run_id}: {external_task_xcom}")

        # Define completion criteria (e.g., any non-None value or specific status)
        if external_task_xcom is not None:
            logger.info(f"Detected completion with recording_status: {external_task_xcom}")
            return True
        
        logger.info(f"Task {self.external_task_id} not yet complete for run_id {trigger_run_id}")
        return False

# Define DAG
with DAG(
    "autoloan_reminder",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    def fetch_due_loans(**kwargs):
        """Fetches loans that are due from the Autoloan API."""
        try:
            logger.info(f"Calling API to fetch due loans: {AUTOLOAN_API_URL}loan/overdue")
            response = requests.get(f"{AUTOLOAN_API_URL}loan/overdue")
            if response.status_code == 200:
                loan_data = response.json()
                overdue_loans = loan_data.get("overdue_loans", [])
                if overdue_loans:
                    first_loan = overdue_loans[0]
                    customer_id = first_loan["customerid"]
                    customer_response = requests.get(f"{AUTOLOAN_API_URL}customer/{customer_id}")
                    if customer_response.status_code == 200:
                        first_loan["phone"] = TEST_PHONE_NUMBER
                    else:
                        raise Exception(f"Failed to fetch customer details for ID {customer_id}")
                kwargs['ti'].xcom_push(key='due_loans', value=[first_loan])
            else:
                raise Exception("Failed to fetch due loans from API")
        except Exception as e:
            logger.error(f"Failed to fetch due loans: {e}")

    def generate_voice_message(**kwargs):
        """Generates voice message content for each loan."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='due_loans')
        if not loans or not isinstance(loans, list):
            logger.error("No loans found to process")
            return
        messages = {
            "phone_number": loans[0]["phone"],
            "message": "Your loan is due, please pay as soon as possible.",
            "need_ack": True
        }
        ti.xcom_push(key='voice_message_payload', value=messages)

    def update_reminder_status(**kwargs):
        """Marks the reminder as scheduled in the Autoloan API."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='due_loans')
        for loan in loans:
            logger.info(f"Updated reminder status for Loan ID: {loan['loanid']}")

    def update_call_status(**kwargs):
        """Updates the call status in the Autoloan API."""
        ti = kwargs['ti']
        recording_status = ti.xcom_pull(dag_id="twilio_voice_call_direct", task_ids='fetch_and_save_recording', key='recording_status')
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='due_loans')
        logger.info(f"Updated call status for Loan ID: {loans[0]['loanid']} with recording status: {recording_status}")

    def trigger_with_run_id(context, dag_run_obj):
        """Custom trigger function to push the triggered run_id to XCom."""
        ti = context['ti']
        run_id = dag_run_obj.run_id
        ti.xcom_push(key='triggered_run_id', value=run_id)
        logger.info(f"Triggered DAG twilio_voice_call_direct with run_id: {run_id}")
        return dag_run_obj

    # Tasks
    fetch_due_loans_task = PythonOperator(
        task_id="fetch_due_loans",
        python_callable=fetch_due_loans,
        provide_context=True,
    )

    generate_voice_message_task = PythonOperator(
        task_id="generate_voice_message",
        python_callable=generate_voice_message,
        provide_context=True,
    )

    trigger_send_voice_message = TriggerDagRunOperator(
        task_id="trigger_twilio_voice_call",
        trigger_dag_id="twilio_voice_call_direct",
        conf="{{ ti.xcom_pull(task_ids='generate_voice_message', key='voice_message_payload') | tojson }}",
        python_callable=trigger_with_run_id,  # Push run_id to XCom
    )

    update_reminder_status_task = PythonOperator(
        task_id="update_reminder_status",
        python_callable=update_reminder_status,
        provide_context=True,
    )

    wait_for_call_completion = XComExternalTaskSensor(
        task_id="wait_for_call_completion",
        external_dag_id="twilio_voice_call_direct",
        external_task_id="fetch_and_save_recording",
        mode="reschedule",  # Efficient waiting
        timeout=1800,  # 30 minutes timeout
        poke_interval=60,  # Check every minute
    )

    update_call_status_task = PythonOperator(
        task_id="update_call_status",
        python_callable=update_call_status,
        provide_context=True,
    )

    # Task Dependencies
    fetch_due_loans_task >> generate_voice_message_task
    generate_voice_message_task >> trigger_send_voice_message >> update_reminder_status_task
    trigger_send_voice_message >> wait_for_call_completion >> update_call_status_task