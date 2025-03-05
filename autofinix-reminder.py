from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import requests
import re
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
    "retry_delay": timedelta(seconds=15),  # 15 seconds retry delay
}

# API Endpoint and Credentials
AUTOFINIX_API_URL = Variable.get("AUTOFINIX_API_URL")
AGENTOMATIC_API_URL = Variable.get("AGENTOMATIC_API_URL")
AUTOFINIX_TEST_PHONE_NUMBER = Variable.get("AUTOFINIX_TEST_PHONE_NUMBER")

# Ensure API URL is set
if not AUTOFINIX_API_URL:
    raise ValueError("Autoloan API URL is missing. Set it in Airflow Variables.")

# Define DAG
with DAG(
    "autofinix_reminder",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),  # Updated to timedelta(minutes=1)
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
) as dag:

    def fetch_due_loans(**kwargs):
        """Fetches loans that are due from the Autoloan API and retrieves the associated phone number."""
        try:
            ti = kwargs['ti']
            logger.info(f"Calling API to fetch due loans: {AUTOFINIX_API_URL}loan/get_reminder?status=Reminder")
            response = requests.get(f"{AUTOFINIX_API_URL}loan/get_reminder?status=Reminder")
            if response.status_code == 200:
                loan_data = response.json()
                logger.info(f"API response: {loan_data}")
                loans_reminders = loan_data.get("reminders", [])  # Extract the reminders list

                if not loans_reminders:
                    logger.info("No reminders found with status=Reminder.")
                    ti.xcom_push(key='eligible_loans', value=[])
                    return

                # Process reminders and fetch phone numbers
                eligible_loans = []
                for reminder in loans_reminders:
                    customer_id = reminder["customer_id"]
                    logger.info(f"Fetching customer details for ID: {customer_id}")
                    customer_response = requests.get(f"{AUTOFINIX_API_URL}customer/{customer_id}")
                    if customer_response.status_code == 200:
                        customer_data = customer_response.json()
                        reminder["phone"] = AUTOFINIX_TEST_PHONE_NUMBER  # Use TEST_PHONE_NUMBER for now
                        logger.info(f"Updated reminder with phone number: {reminder}")
                        eligible_loans.append(reminder)
                    else:
                        logger.error(f"Failed to fetch customer details for ID {customer_id}")
                        continue

                if not eligible_loans:
                    logger.info("No eligible reminders to process after filtering.")
                    ti.xcom_push(key='eligible_loans', value=[])
                    ti.xcom_push(key='call_outcome', value="Completed")
                    return

                logger.info(f"Eligible reminders: {eligible_loans}")
                ti.xcom_push(key='eligible_loans', value=eligible_loans)  # Push as flat list
            else:
                logger.error(f"Failed to fetch reminders from API: {response.status_code} - {response.text}")
                raise Exception("Failed to fetch reminders from API")
        except Exception as e:
            logger.error(f"Failed to fetch due loans: {e}")
            raise

    def evaluate_due_loans_result(**kwargs):
        """Determines which path to take based on whether eligible loans are found."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='eligible_loans')
        if loans and isinstance(loans, list) and len(loans) > 0:
            return "generate_voice_message"
        else:
            return "handle_no_due_loans"
    
    def generate_voice_message_agent(loan_id):
        client = Client(
        host=AGENTOMATIC_API_URL,
        headers={'x-ltai-client': 'autofinix-voice-respond'}
    )

        response = client.chat(
            model='autofinix:0.3',
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Generate a professional loan due reminder message for loan ID {loan_id}. "
                        "Fetch the overdue details for this loan, including customerid, loanamount, interestrate, "
                        "tenureinmonths, outstandingamount, overdueamount, lastduedate, lastpaiddate, and daysoverdue. "
                        "If specific details are unavailable or cannot be retrieved, use placeholder text or generic terms. "
                        "The final response must be a concise message which should not exceed 500 charecters, containing only relevant content, suitable for conversion to a voice call. The final response should only contain the message and avoid any irrelevant message by model"
                    )
                }
            ],
            stream=False
        )
        agent_response = response['message']['content']
        logging.info(f" Agent Response: {agent_response}")
        return agent_response
    def clean_message(message):
        """
        Removes newline (\n) and tab (\t) characters from the message.
        """
        return re.sub(r'[\n\t]', ' ', message).strip()
    
    def generate_voice_message(**kwargs):
        """Generates voice message content for each loan."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='eligible_loans')
        if not loans or not isinstance(loans, list):
            logger.error("No eligible loans found to process")
            ti.xcom_push(key='call_outcome', value="Failed")
            ti.xcom_push(key='call_ids', value=[])
            return

        call_ids = []
        for loan in loans:
            # Update reminder_status to CallIntiated for each loan
            loan_id = loan['loan_id']
            call_id = loan['call_id']  # Assuming 'id' is the CallID from the API response
            update_url = f"{AUTOFINIX_API_URL}loan/{loan_id}/update_reminder"
            params = {"status": "CallInitiated", "call_id": call_id}  # Corrected parameter name
            try:
                response = requests.put(update_url, params=params)
                if response.status_code == 200:
                    logger.info(f"Updated reminder_status to CallInitiated for loan ID: {loan_id}, call ID: {call_id}")
                    result = response.json()
                    updated_call_id = result.get('call_id')  # Get the call_id from the API response
                    if not updated_call_id:
                        logger.error(f"Call ID not returned in API response for loan ID: {loan_id}")
                        raise Exception("Call ID not returned in API response")
                    call_id = updated_call_id  # Update call_id if API confirms a different one
                else:
                    logger.error(f"Failed to update reminder_status to CallInitiated for loan ID: {loan_id}. Status: {response.status_code}, Response: {response.text}")
                    raise Exception(f"API failure: {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"Failed to update reminder_status to CallInitiated: {str(e)}")
                ti.xcom_push(key='call_outcome', value="Failed")
                ti.xcom_push(key='call_ids', value=[])
                raise  # Fail the task explicitly to stop the DAG run
            
            # Generate voice message using Agent-O-Matic and clean the message
            agent_cleaned_message=clean_message(generate_voice_message_agent(loan_id))
            # Use the call_id from the API response
            messages = {
                "phone_number": loan["phone"],
                "message": agent_cleaned_message,
                "need_ack": True,
                "call_id": call_id  # Use API-provided call_id
            }
            ti.xcom_push(key=f'voice_message_payload_{call_id}', value=messages)
            ti.xcom_push(key=f'call_id_{call_id}', value=call_id)
            ti.xcom_push(key=f'loan_id_{call_id}', value=loan_id)
            logger.info(f"Using call_id: {call_id} for loan_id: {loan_id}")
            call_ids.append(str(call_id))

        ti.xcom_push(key='call_ids', value=call_ids)

    def trigger_twilio_voice_call(**kwargs):
        """Trigger `send-voice-message` DAG and retrieve call status using `call_id`."""
        ti = kwargs.get("ti")
        if not ti:
            logger.error("TaskInstance (ti) not available in kwargs")
            raise ValueError("TaskInstance (ti) missing in kwargs")

        call_outcome = ti.xcom_pull(task_ids="generate_voice_message", key="call_outcome")
        if call_outcome == "Failed":
            logger.info("Skipping trigger due to failure in generate_voice_message")
            ti.xcom_push(key="call_outcome", value="Failed")
            return

        call_ids = ti.xcom_pull(task_ids="generate_voice_message", key="call_ids") or []
        if not call_ids:
            logger.info("No call_ids available, skipping trigger")
            ti.xcom_push(key="call_outcome", value="Failed")
            return

        final_outcomes = {}

        for call_id in call_ids:
            conf = ti.xcom_pull(task_ids="generate_voice_message", key=f"voice_message_payload_{call_id}")
            if not conf:
                logger.info(f"No voice message payload available for call_id {call_id}, skipping")
                final_outcomes[call_id] = "Failed"
                continue

            logger.info(f"Triggering `send-voice-message` with conf: {conf}")

            # Pass `call_id` so it can be used in `send-voice-message`
            conf["call_id"] = call_id

            # Trigger `send-voice-message` DAG
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_twilio_voice_call_inner_{call_id}",
                trigger_dag_id="send-voice-message",
                conf=conf,
                wait_for_completion=True,
                poke_interval=30,
            )
            trigger.execute(kwargs)

            # ✅ Fetch call status using `call_id`
            call_status = ti.xcom_pull(task_ids="check_call_status", key=f"call_status_{call_id}")
            logger.info(f"Call ID: {call_id}, Call status received: {call_status}")

            # Map Twilio status to custom database status
            if call_status == "completed":
                final_outcomes[call_id] = "CallCompleted"
            elif call_status in ["no-answer", "busy", "failed"]:
                final_outcomes[call_id] = "CallFailed"
            else:
                final_outcomes[call_id] = "Unknown"

        # Push all final call outcomes to XCom
        ti.xcom_push(key="final_call_outcomes", value=final_outcomes)

    def update_call_status(**kwargs):
        ti = kwargs['ti']
        call_ids = ti.xcom_pull(task_ids='generate_voice_message', key='call_ids') or []

        for call_id in call_ids:
            # Retrieve the Twilio call status from the Variable
            twilio_status = Variable.get(f"twilio_call_status_{call_id}", default_var=None)
            if not twilio_status:
                # If for some reason it doesn't exist, assume "Failed"
                twilio_status = "failed"
            logger.info(f"Twilio status for call_id={call_id}: {twilio_status}")
            # Map Twilio status to your custom DB statuses
            if twilio_status == "completed":
                reminder_status = "CallCompleted"
            elif twilio_status in "no-answer":
                reminder_status = "CallNotAnswered"
            else:
                reminder_status = "CallFailed"

            loan_id = ti.xcom_pull(task_ids='generate_voice_message', key=f'loan_id_{call_id}')
            logger.info(f"Updating reminder status to {reminder_status} for call_id={call_id}, loan_id={loan_id}")
            try:
                # Update your loan DB or API
                update_url = f"{AUTOFINIX_API_URL}loan/{loan_id}/update_reminder"
                params = {"status": reminder_status, "call_id": call_id}
                response = requests.put(update_url, params=params)
                if response.status_code == 200:
                    logger.info(f"Updated status to {reminder_status} for call_id={call_id}, loan_id={loan_id}")
                else:
                    logger.error(f"Failed to update status. call_id={call_id}, loan_id={loan_id} Status: {response.status_code}, Response: {response.text}")
            except Exception as e:
                logger.error(f"Failed to update status. call_id={call_id}, loan_id={loan_id} due to the exception: {str(e)}")
            # OPTIONAL: Delete the Variable now that we have stored its contents
            Variable.delete(f"twilio_call_status_{call_id}")
            logger.info(f"Deleted Variable key twilio_call_status_{call_id} to avoid clutter.")
        
        
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
            if final_call_outcome == "Success":
                logger.info(f"Call reminder succeeded for Loan ID: {loan_id}")
            else:
                logger.info(f"Call reminder failed for Loan ID: {loan_id}")

    # Tasks
    fetch_due_loans_task = PythonOperator(
        task_id="fetch_due_loans",
        python_callable=fetch_due_loans,
        provide_context=True,
    )

    evaluate_due_loans_results = BranchPythonOperator(
        task_id="evaluate_due_loans_result",
        python_callable=evaluate_due_loans_result,
        provide_context=True,
    )

    handle_no_due_loans = DummyOperator(
        task_id="handle_no_due_loans",
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

    end_task = DummyOperator(
        task_id="end_task",
        trigger_rule="all_done",  # Runs regardless of upstream status
    )

    # Task Dependencies
    fetch_due_loans_task >> evaluate_due_loans_results
    evaluate_due_loans_results >> [generate_voice_message_task, handle_no_due_loans]
    generate_voice_message_task >> trigger_send_voice_message
    trigger_send_voice_message >> update_call_status_task
    update_call_status_task >> update_reminder_status_task
    update_reminder_status_task >> end_task
    handle_no_due_loans >> end_task
