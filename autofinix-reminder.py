from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
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

# Define DAG
with DAG(
    "autoloan_reminder",
    default_args=default_args,
    schedule_interval=None,  # Manually triggered or via scheduler
    catchup=False,
) as dag:

    def fetch_due_loans(**kwargs):
        """Fetches loans that are due from the Autoloan API and retrieves the associated phone number."""
        try:
            logger.info(f"Calling API to fetch due loans: {AUTOLOAN_API_URL}loan/overdue")
            response = requests.get(f"{AUTOLOAN_API_URL}loan/overdue")
            if response.status_code == 200:
                loan_data = response.json()
                overdue_loans = loan_data.get("overdue_loans", [])  # Extract the overdue loans list

                if overdue_loans:
                    first_loan = overdue_loans[0]  # Get the first record
                    logger.info(f"First overdue loan details: {first_loan}")

                    customer_id = first_loan["customerid"]
                    logger.info(f"Fetching customer details for ID: {customer_id}")

                    # Fetch customer details
                    customer_response = requests.get(f"{AUTOLOAN_API_URL}customer/{customer_id}")

                    if customer_response.status_code == 200:
                        customer_data = customer_response.json()
                        first_loan["phone"] = TEST_PHONE_NUMBER  # Assign test phone number
                        logger.info(f"Updated first loan with test phone number: {first_loan}")
                    else:
                        raise Exception(f"Failed to fetch customer details for ID {customer_id}")

                kwargs['ti'].xcom_push(key='due_loans', value=[first_loan])  # Ensure it's a list
            else:
                logger.error(f"Failed to fetch due loans from API: {response.text}")
                raise Exception("Failed to fetch due loans from API")
        except Exception as e:
            logger.error(f"Failed to fetch due loans: {e}")

    def generate_message_using_agent(loan):
        """Generates voice message content for each loan."""
        try:
            return "Your loan is due, please pay as soon as possible."
        except Exception as e:
            logging.error(f"Failed to generate message using agent: {e}")
            return f"Hello, this is a reminder that your loan is due. Please contact us for more information."

    def generate_voice_message(**kwargs):
        """Generates voice message content for each loan."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='due_loans')

        if not loans or not isinstance(loans, list):
            logger.error("No loans found to process")
            return

        messages = {
            "phone_number": loans[0]["phone"],  # Pass only the first loan for now
            "message": generate_message_using_agent(loans[0]),
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
        wait_for_completion=False,
        execution_date="{{ dag_run.start_date }}",  # Pass the execution date explicitly
        reset_dag_run=True,  # Ensure a new run is created
    )

    update_reminder_status_task = PythonOperator(
        task_id="update_reminder_status",
        python_callable=update_reminder_status,
        provide_context=True,
    )

    # Custom execution_date_fn to match the triggered DAG run
    def get_triggered_execution_date(context):
        """Returns the execution date of the triggered twilio_voice_call_direct DAG."""
        trigger_run_id = context['ti'].xcom_pull(task_ids='trigger_twilio_voice_call', key='run_id')
        from airflow.models import DagRun
        triggered_dag_run = DagRun.find(dag_id="twilio_voice_call_direct", run_id=trigger_run_id)
        return triggered_dag_run[0].execution_date if triggered_dag_run else context['execution_date']

    wait_for_call_completion = ExternalTaskSensor(
        task_id="wait_for_call_completion",
        external_dag_id="twilio_voice_call_direct",
        external_task_id="fetch_and_save_recording",
        execution_date_fn=get_triggered_execution_date,  # Match the triggered DAG's execution date
        mode="reschedule",  # More efficient than poke
        timeout=1800,  # 30 minutes timeout to account for call duration
        poke_interval=60,  # Check every minute
        check_existence=True,  # Ensure the task exists before waiting
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