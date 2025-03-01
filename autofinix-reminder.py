from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import requests
import logging
import uuid

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
        call_id = str(uuid.uuid4())
        messages = {
            "phone_number": loans[0]["phone"],
            "message": "Your loan is due, please pay as soon as possible.",
            "need_ack": True,
            "call_id": call_id
        }
        ti.xcom_push(key='voice_message_payload', value=messages)
        ti.xcom_push(key='call_id', value=call_id)

    def update_call_status(**kwargs):
        """Validates call status and prepares it for reminder update."""
        ti = kwargs['ti']
        call_id = ti.xcom_pull(task_ids='generate_voice_message', key='call_id')
        logger.info(f"Attempting to pull recording_status for call_id: {call_id}")

        if not call_id:
            logger.error("call_id is None in update_call_status")
            ti.xcom_push(key='call_outcome', value="Failed")
            return

        recording_status = ti.xcom_pull(
            dag_id="twilio_voice_call_direct",
            task_ids='fetch_and_save_recording',
            key=f'recording_status_{call_id}'
        )

        logger.info(f"Pulled recording_status for call_id {call_id}: {recording_status}")

        if recording_status is None:
            logger.error(f"No recording_status found for call_id {call_id}")
            ti.xcom_push(key='call_outcome', value="Failed")
        else:
            logger.info(f"Call status for call_id {call_id}: {recording_status}")
            ti.xcom_push(key='call_outcome', value="Success" if recording_status == "Recording Saved" else "Failed")

    def update_reminder_status(**kwargs):
        """Updates the reminder status based on call outcome."""
        ti = kwargs['ti']
        call_outcome = ti.xcom_pull(task_ids='update_call_status', key='call_outcome')
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='due_loans')
        
        if call_outcome == "Success":
            logger.info(f"Call reminder succeeded for Loan ID: {loans[0]['loanid']}")
            # Future API integration: Update DB column to "Call Reminder Success"
        else:
            logger.info(f"Call reminder failed for Loan ID: {loans[0]['loanid']}")
            # Future API integration: Update DB column to "Call Reminder Failed")

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
        wait_for_completion=True,
        poke_interval=60,
    )

    update_call_status_task = PythonOperator(
        task_id="update_call_status",
        python_callable=update_call_status,
        provide_context=True,
    )

    update_reminder_status_task = PythonOperator(
        task_id="update_reminder_status",
        python_callable=update_reminder_status,
        provide_context=True,
    )

    # Task Dependencies
    fetch_due_loans_task >> generate_voice_message_task
    generate_voice_message_task >> trigger_send_voice_message
    trigger_send_voice_message >> update_call_status_task >> update_reminder_status_task