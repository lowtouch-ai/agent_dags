from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import requests
import logging
import uuid
from ollama import Client 
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
    "autofinix_reminder",
    default_args=default_args,
    schedule_interval="* * * * *",
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
) as dag:

    def fetch_due_loans(**kwargs):
        """Fetches loans that are due from the Autoloan API and retrieves the associated phone number."""
        try:
            ti = kwargs['ti']
            logger.info(f"Calling API to fetch due loans: {AUTOLOAN_API_URL}loan/get_reminder?status=Reminder")
            response = requests.get(f"{AUTOLOAN_API_URL}loan/get_reminder?status=Reminder")
            if response.status_code == 200:
                loan_data = response.json()
                logger.info(f"API response: {loan_data}")
                loans_reminders = loan_data.get("reminders", [])  # Extract the overdue loans list

                if not loans_reminders:
                    logger.info("No overdue loans found with reminder_status=Reminder.")
                    ti.xcom_push(key='eligible_loans', value=[])
                    return

                # Process loans and fetch phone numbers
                eligible_loans = []
                for loan in loans_reminders:

                    customer_id = loan["customer_id"]
                    logger.info(f"Fetching customer details for ID: {customer_id}")
                    customer_response = requests.get(f"{AUTOLOAN_API_URL}customer/{customer_id}")
                    if customer_response.status_code == 200:
                        customer_data = customer_response.json()
                        loan["phone"] = TEST_PHONE_NUMBER  # Use TEST_PHONE_NUMBER for now
                        logger.info(f"Updated loan with phone number: {loan}")
                        eligible_loans.append(loan)
                    else:
                        logger.error(f"Failed to fetch customer details for ID {customer_id}")
                        continue

                if not eligible_loans:
                    logger.info("No eligible loans to process after filtering.")
                    ti.xcom_push(key='eligible_loans', value=[])
                    ti.xcom_push(key='call_outcome', value="Completed")
                    return

                
                logger.info(f"Processing first eligible loan: {eligible_loans}")
                ti.xcom_push(key='eligible_loans', value=eligible_loans)
            else:
                logger.error(f"Failed to fetch due loans from API: {response.status_code} - {response.text}")
                raise Exception("Failed to fetch due loans from API")
        except Exception as e:
            logger.error(f"Failed to fetch due loans: {e}")
            raise
    def generate_voice_message_agent(loan_id):
        client = Client(
        host='http://agentomatic:8000',
        headers={'x-ltai-client': 'autofinix-voice-respond'}
    )

        response = client.chat(
            model='autofinix:0.3',
            messages=[{"role": "user", "content": 'Need to generate a loan due message for the loanid:{loan_id}'}],
            stream=False
        )
        agent_response = response['message']['content']
        logging.info(f" Agent Response: {agent_response}")
        return agent_response
    def generate_voice_message(**kwargs):
        """Generates voice message content for each loan."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='eligible_loans')
        if not loans or not isinstance(loans, list):
            logger.error("No eligible loans found to process")
            ti.xcom_push(key='call_outcome', value="Failed")
            return
        for loan in loans:
            # Update reminder_status to ReminderSent for each loan
            loan_id = loan['loan_id']
            update_url = f"{AUTOLOAN_API_URL}loan/{loan_id}/update_reminder"
            params = {"status": "CallInitiated"}  # Matches updated API valid values
            try:
                response = requests.put(update_url, params=params)
                if response.status_code == 200:
                    logger.info(f"Updated reminder_status to CallIntiated for loan ID: {loan_id}")
                else:
                    logger.error(f"Failed to update reminder_status to CallIntiated for loan ID: {loan_id}. Status: {response.status_code}, Response: {response.text}")
                    raise Exception(f"API failure: {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"Failed to update reminder_status to ReminderSent: {str(e)}")
                ti.xcom_push(key='call_outcome', value="Failed")
                raise  # Fail the task explicitly to stop the DAG run

            call_id = str(uuid.uuid4())
            messages = {
                "phone_number": loan["phone"],  # Use the fetched phone number
                "message": generate_voice_message_agent(loan_id),
                "need_ack": True,
                "call_id": call_id
            }
            ti.xcom_push(key='voice_message_payload', value=messages)
            ti.xcom_push(key='call_id', value=call_id)
            ti.xcom_push(key='loan_id', value=loan_id)  # Push loan_id for later use
            logger.info(f"Generated call_id: {call_id} for loan_id: {loan_id}")

    def trigger_twilio_voice_call(**kwargs):
        """Trigger send-voice-message and push outcome to XCom."""
        ti = kwargs.get('ti')
        if not ti:
            logger.error("TaskInstance (ti) not available in kwargs")
            raise ValueError("TaskInstance (ti) missing in kwargs")

        call_outcome = ti.xcom_pull(task_ids='generate_voice_message', key='call_outcome')
        if call_outcome == "Failed":
            logger.info("Skipping trigger due to failure in generate_voice_message")
            ti.xcom_push(key='call_outcome', value="Failed")
            return

        conf = ti.xcom_pull(task_ids='generate_voice_message', key='voice_message_payload')
        if not conf:
            logger.info("No voice message payload available, skipping trigger")
            ti.xcom_push(key='call_outcome', value="Failed")
            return

        logger.info(f"Triggering send-voice-message with conf: {conf}")
        
        trigger = TriggerDagRunOperator(
            task_id="trigger_twilio_voice_call_inner",
            trigger_dag_id="send-voice-message",
            conf=conf,
            wait_for_completion=True,
            poke_interval=30,
        )
        trigger.execute(kwargs)
        
        # Since wait_for_completion=True, if we reach here, the triggered DAG succeeded
        logger.info("send-voice-message completed successfully")
        ti.xcom_push(key='call_outcome', value="Success")

    def update_call_status(**kwargs):
        """Update reminder_status based on call outcome."""
        ti = kwargs['ti']
        call_outcome = ti.xcom_pull(task_ids='trigger_twilio_voice_call', key='call_outcome')
        if call_outcome == "Failed":
            logger.info("Skipping update due to failure in trigger_twilio_voice_call")
            ti.xcom_push(key='final_call_outcome', value="Failed")
            return

        loan_id = ti.xcom_pull(task_ids='generate_voice_message', key='loan_id')
        if not loan_id:
            logger.error("loan_id not found in XCom")
            ti.xcom_push(key='final_call_outcome', value="Failed")
            return

        update_url = f"{AUTOLOAN_API_URL}loan/{loan_id}/update_reminder"
        if call_outcome == "Success":
            reminder_status = "CalledCompleted"
        else:
            reminder_status = "CallFailed"  # Assuming failure means no-answer; can refine later

        params = {"status": reminder_status}
        try:
            response = requests.put(update_url, params=params)
            if response.status_code == 200:
                logger.info(f"Updated reminder_status to {reminder_status} for loan ID: {loan_id}")
                ti.xcom_push(key='final_call_outcome', value=reminder_status)
            else:
                logger.error(f"Failed to update reminder_status to {reminder_status} for loan ID: {loan_id}. Status: {response.status_code}, Response: {response.text}")
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
        for loan in loans:
            loan_id = loan['loan_id']
            if final_call_outcome == "Called":
                logger.info(f"Call reminder succeeded and status set to Called for Loan ID: {loan_id}")
            elif final_call_outcome == "Failed":
                logger.info(f"Call reminder failed for Loan ID: {loan_id}")
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