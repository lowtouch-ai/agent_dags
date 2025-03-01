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
        """Fetches loans that are due from the Autoloan API and filters by reminder_status."""
        try:
            ti = kwargs['ti']
            logger.info(f"Calling API to fetch due loans: {AUTOLOAN_API_URL}loan/overdue")
            response = requests.get(f"{AUTOLOAN_API_URL}loan/overdue")
            if response.status_code == 200:
                loan_data = response.json()
                overdue_loans = loan_data.get("overdue_loans", [])
                if not overdue_loans:
                    logger.info("No overdue loans found.")
                    return

                # Filter loans based on reminder_status
                eligible_loans = []
                for loan in overdue_loans:
                    reminder_status = loan.get("reminder_status", "NotCalled")  # Default if not present
                    if reminder_status == "Completed":
                        logger.info(f"Loan ID {loan['loanid']} already completed, skipping.")
                        continue
                    eligible_loans.append(loan)

                if not eligible_loans:
                    logger.info("No eligible loans to process after filtering.")
                    ti.xcom_push(key='eligible_loans', value=[])
                    ti.xcom_push(key='call_outcome', value="Completed")
                    return

                # For testing, take the first eligible loan
                first_loan = eligible_loans[0]
                logger.info(f"Processing first eligible loan: {first_loan}")
                ti.xcom_push(key='eligible_loans', value=[first_loan])
            else:
                raise Exception("Failed to fetch due loans from API")
        except Exception as e:
            logger.error(f"Failed to fetch due loans: {e}")
            raise

    def generate_voice_message(**kwargs):
        """Generates voice message content for each loan."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='eligible_loans')
        if not loans or not isinstance(loans, list):
            logger.error("No eligible loans found to process")
            ti.xcom_push(key='call_outcome', value="Failed")
            return

        # Update reminder_status to ReminderSent for each loan
        loan = loans[0]  # First loan for testing
        loan_id = loan['loanid']
        update_url = f"{AUTOLOAN_API_URL}loan/{loan_id}/update_reminder"
        params = {"reminder_status": "ReminderSent"}
        try:
            response = requests.put(update_url, params=params)
            if response.status_code == 200:
                logger.info(f"Updated reminder_status to ReminderSent for loan ID: {loan_id}")
            else:
                logger.error(f"Failed to update reminder_status to ReminderSent for loan ID: {loan_id}")
                ti.xcom_push(key='call_outcome', value="Failed")
                return
        except Exception as e:
            logger.error(f"Failed to update reminder_status to ReminderSent: {e}")
            ti.xcom_push(key='call_outcome', value="Failed")
            return

        call_id = str(uuid.uuid4())
        messages = {
            "phone_number": loan["phone"],
            "message": "Your loan is due, please pay as soon as possible.",
            "need_ack": True,
            "call_id": call_id
        }
        ti.xcom_push(key='voice_message_payload', value=messages)
        ti.xcom_push(key='call_id', value=call_id)
        ti.xcom_push(key='loan_id', value=loan_id)  # Push loan_id for later use
        logger.info(f"Generated call_id: {call_id} for loan_id: {loan_id}")

    def trigger_twilio_voice_call(**kwargs):
        """Trigger twilio_voice_call_direct and push outcome to XCom."""
        ti = kwargs.get('ti')
        if not ti:
            logger.error("TaskInstance (ti) not available in kwargs")
            raise ValueError("TaskInstance (ti) missing in kwargs")

        logger.info(f"Triggering twilio_voice_call_direct with conf: {ti.xcom_pull(task_ids='generate_voice_message', key='voice_message_payload')}")
        
        trigger = TriggerDagRunOperator(
            task_id="trigger_twilio_voice_call_inner",
            trigger_dag_id="twilio_voice_call_direct",
            conf=ti.xcom_pull(task_ids='generate_voice_message', key='voice_message_payload'),
            wait_for_completion=True,
            poke_interval=60,
        )
        trigger.execute(kwargs)
        
        # Since wait_for_completion=True, if we reach here, the triggered DAG succeeded
        logger.info("twilio_voice_call_direct completed successfully")
        ti.xcom_push(key='call_outcome', value="Success")

    def update_call_status(**kwargs):
        """Update reminder_status based on call outcome."""
        ti = kwargs['ti']
        call_outcome = ti.xcom_pull(task_ids='trigger_twilio_voice_call', key='call_outcome')
        loan_id = ti.xcom_pull(task_ids='generate_voice_message', key='loan_id')

        if not loan_id:
            logger.error("loan_id not found in XCom")
            ti.xcom_push(key='final_call_outcome', value="Failed")
            return

        update_url = f"{AUTOLOAN_API_URL}loan/{loan_id}/update_reminder"
        if call_outcome == "Success":
            reminder_status = "Called"
        else:
            reminder_status = "Failed"  # For now, assuming failure means no-answer; can refine later

        params = {"reminder_status": reminder_status}
        try:
            response = requests.put(update_url, params=params)
            if response.status_code == 200:
                logger.info(f"Updated reminder_status to {reminder_status} for loan ID: {loan_id}")
                ti.xcom_push(key='final_call_outcome', value=reminder_status)
            else:
                logger.error(f"Failed to update reminder_status to {reminder_status} for loan ID: {loan_id}")
                ti.xcom_push(key='final_call_outcome', value="Failed")
        except Exception as e:
            logger.error(f"Failed to update reminder_status: {e}")
            ti.xcom_push(key='final_call_outcome', value="Failed")

    def update_reminder_status(**kwargs):
        """Logs the final reminder status based on call outcome."""
        ti = kwargs['ti']
        final_call_outcome = ti.xcom_pull(task_ids='update_call_status', key='final_call_outcome')
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='eligible_loans')
        
        if not loans:
            logger.info("No eligible loans processed.")
            return

        loan_id = loans[0]['loanid']
        if final_call_outcome == "Called":
            logger.info(f"Call reminder succeeded and status set to Called for Loan ID: {loan_id}")
            # Future API integration: Update DB column to "Call Reminder Success"
        elif final_call_outcome.startswith("Failed"):
            logger.info(f"Call reminder failed for Loan ID: {loan_id}")
            # Future API integration: Update DB column to "Call Reminder Failed"
        else:
            logger.info(f"Call reminder status set to {final_call_outcome} for Loan ID: {loan_id}")

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

    trigger_send_voice_message = PythonOperator(
        task_id="trigger_twilio_voice_call",
        python_callable=trigger_twilio_voice_call,
        provide_context=True,
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
    trigger_send_voice_message >> update_call_status_task
    update_call_status_task >> update_reminder_status_task