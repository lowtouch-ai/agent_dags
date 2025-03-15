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
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default args for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

# API Endpoint and Credentials from Airflow Variables
AUTOFINIX_API_URL = Variable.get("AUTOFINIX_API_URL")
AGENTOMATIC_API_URL = Variable.get("AGENTOMATIC_API_URL")
AUTOFINIX_TEST_PHONE_NUMBER = Variable.get("AUTOFINIX_TEST_PHONE_NUMBER")
AUTOFINIX_DEMO_PHONE_ODD=Variable.get("AUTOFINIX_DEMO_PHONE_ODD")
AUTOFINIX_DEMO_PHONE_EVEN=Variable.get("AUTOFINIX_DEMO_PHONE_EVEN")


if not AUTOFINIX_API_URL:
    raise ValueError("Autoloan API URL is missing. Set it in Airflow Variables.")

def make_api_request(url, method="GET", params=None, json=None, retries=3):
    """Helper function for API requests with timeout and retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    try:
        if method == "GET":
            response = session.get(url, params=params, timeout=10)
        elif method == "PUT":
            response = session.put(url, json=json, params=params, timeout=10)
        elif method == "POST":
            response = session.post(url, json=json, params=params, timeout=10)
        else:
            raise ValueError(f"Unsupported method: {method}")

        response.raise_for_status()
        return response.json() if response.content else {}
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

def fetch_due_loans(api_url, test_phone_number,even_phone_number,odd_phone_number, **kwargs):
    """Fetches loans that are due from the Autoloan API"""
    ti = kwargs['ti']
    try:
        url = f"{api_url}loan/get_reminder?reminder_flag=true&status=Reminder"
        logger.info(f"Calling API to fetch due loans: {url}")
        loan_data = make_api_request(url)
        
        loans_reminders = loan_data.get("reminders", [])
        if not loans_reminders:
            logger.info("No reminders found with status=Reminder.")
            ti.xcom_push(key='eligible_loans', value=[])
            return

        eligible_loans = []
        for reminder in loans_reminders:
            customer_id = reminder["customer_id"]
            logger.info(f"Fetching customer details for ID: {customer_id}")
            customer_data = make_api_request(f"{api_url}customer/{customer_id}")
            if customer_data:
                # Rename remind_on to inserted_timestamp for consistency
                reminder["inserted_timestamp"] = reminder.pop("remind_on")
                if int(reminder['loan_id'])%2==0:
                    if int(reminder['loan_id'])==550:
                        reminder["phone"] = test_phone_number
                    else:
                        reminder["phone"] = even_phone_number
                else:
                    reminder["phone"] = odd_phone_number
                
                logger.info(f"Updated reminder with phone number and timestamp: {reminder}")
                eligible_loans.append(reminder)

        if not eligible_loans:
            logger.info("No eligible reminders to process after filtering.")
            ti.xcom_push(key='eligible_loans', value=[])
            ti.xcom_push(key='call_outcome', value="Completed")
            return

        logger.info(f"Eligible reminders: {eligible_loans}")
        ti.xcom_push(key='eligible_loans', value=eligible_loans)
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

def generate_voice_message_agent(loan_id, agent_url, transcription=None, inserted_date=None):
    """Calls the AgentOmatic API to generate a message or analyze transcription."""
    client = Client(
        host=agent_url,
        headers={'x-ltai-client': 'autofinix-voice-respond'}
    )
    if transcription and inserted_date:
        prompt = (
            f"Analyze the following transcription: '{transcription}' to extract a date for the next loan reminder. "
            f"Interpret relative time expressions (e.g., 'next week,' 'tomorrow') based on the current date: {inserted_date}. "
            f"For 'next week,' use the same weekday as the current date in the following week (e.g., if today is Wednesday, 'next week' is next Wednesday). "
            f"If a specific date or clear intent to pay is found, calculate that date; if it is more than 2 months (120 days) from {inserted_date}, treat it as unrealistic and set the date one week from {inserted_date}. "
            f"If the transcription indicates a refusal to pay (e.g., 'won’t pay,' 'can’t pay,' 'refuse,' 'at any cost'), set the date one week from {inserted_date}. "
            f"If no date is found and no refusal is detected, set the date one month from {inserted_date}. "
            f"Return only the date in ISO format (e.g., '2025-03-19T12:00:00+00:00') as a string, with no additional text."
        )
    else:
        prompt = (
            f"""Generate a professional loan due reminder message for loan ID {loan_id}. Fetch overdue details of this loan including customerid, loanamount, interestrate, tenureinmonths, outstandingamount, overdueamount, lastduedate, lastpaiddate, and daysoverdue. If details are unavailable, use placeholders (e.g., 'Customer', 'N days'). Convert the message into this template: 'Dear (customer_name), this is a gentle reminder of your loan number ({loan_id}) which is overdue by (n) installments. Kindly tell us when you can make the payments, after the beep'.The final response must be a concise message which should not exceed 500 characters, containing only relevant content, suitable for conversion to a voice call."""
        )
    response = client.chat(
        model='autofinix:0.3',
        messages=[{"role": "user", "content": prompt}],
        stream=False
    )
    agent_response = response['message']['content']
    logging.info(f"AgentOmatic Response: {agent_response}")
    return agent_response

def clean_message(message):
    """
    Removes newline (\n) and tab (\t) characters from the message.
    """
    return re.sub(r'[\n\t]', ' ', message).strip()
def extract_final_datetime(text):
    """
    Extracts the datetime from the text. If multiple dates are present, 
    returns the date from the last sentence. If no date is found, returns None.
    Args:
        text (str): The input text containing one or more datetimes.
    
    Returns:
        str | None: The extracted ISO datetime string or None if no date is found.
    """
    # Regular expression for ISO datetime (e.g., 2025-04-09T23:16:00+00:00)
    date_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\+\d{2}:\d{2})?'
    
    # Split text into sentences (using period, newline, or other sentence delimiters)
    sentences = re.split(r'[.\n]+', text.strip())
    
    # Reverse iterate through sentences to find the last valid date
    for sentence in reversed(sentences):
        dates = re.findall(date_pattern, sentence)
        if dates:  # If the sentence contains at least one date
            return dates[-1]  # Return the last date in that sentence

    # If no sentences have dates, check the entire text for a single date
    dates = re.findall(date_pattern, text)
    return dates[0] if dates else None
def generate_voice_message(api_url, agent_url, **kwargs):
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
        loan_id = loan['loan_id']
        call_id = loan['call_id']
        update_url = f"{api_url}loan/{loan_id}/update_reminder"
        params = {"status": "CallInitiated", "call_id": call_id}
        
        try:
            result = make_api_request(update_url, method="PUT", params=params)
            updated_call_id = result.get('call_id')
            if not updated_call_id:
                logger.error(f"Call ID not returned in API response for loan ID: {loan_id}")
                raise Exception("Call ID not returned in API response")
            call_id = updated_call_id
            logger.info(f"Updated reminder_status to CallInitiated for loan ID: {loan_id}, call ID: {call_id}")
            
            message_content = generate_voice_message_agent(loan_id=loan_id, agent_url=agent_url)
            messages = {
                "phone_number": loan["phone"],
                "message": message_content,
                "need_ack": True,
                "call_id": call_id
            }
            ti.xcom_push(key=f'voice_message_payload_{call_id}', value=messages)
            ti.xcom_push(key=f'call_id_{call_id}', value=call_id)
            ti.xcom_push(key=f'loan_id_{call_id}', value=loan_id)
            logger.info(f"Using call_id: {call_id} for loan_id: {loan_id}")
            call_ids.append(str(call_id))
        except Exception as e:
            logger.error(f"Failed to update reminder_status: {str(e)}")
            ti.xcom_push(key='call_outcome', value="Failed")
            ti.xcom_push(key='call_ids', value=[])
            raise

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

    for call_id in call_ids:
        conf = ti.xcom_pull(task_ids="generate_voice_message", key=f"voice_message_payload_{call_id}")
        if not conf:
            logger.info(f"No voice message payload available for call_id {call_id}, skipping")
            continue

        logger.info(f"Triggering `send-voice-message` DAG with conf: {conf}")
        conf["call_id"] = call_id

        # Trigger `send-voice-message` DAG
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_twilio_voice_call_inner_{call_id}",
            trigger_dag_id="send-voice-message-transcript",
            conf=conf,
            wait_for_completion=True,
            poke_interval=30,
        )
        trigger.execute(kwargs)

def update_call_status(api_url, agent_url, **kwargs):
    """Updates call status, analyzes transcription, and sets new reminder."""
    ti = kwargs['ti']
    call_ids = ti.xcom_pull(task_ids='generate_voice_message', key='call_ids') or []
    final_outcomes = {}

    # Fetch reminders to get inserted_timestamp
    loans = ti.xcom_pull(task_ids='fetch_due_loans', key='eligible_loans') or []
    call_id_to_inserted_date = {str(loan['call_id']): loan.get('inserted_timestamp') for loan in loans}
    logger.info(f"Call ID to inserted_date mapping: {call_id_to_inserted_date}")

    for call_id in call_ids:
        # Get status from Variable set by send-voice-message
        twilio_status = Variable.get(f"twilio_call_status_{call_id}", default_var=None)
        if not twilio_status:
            twilio_status = "failed"
        logger.info(f"Twilio status for call_id={call_id}: {twilio_status}")

        # Get transcription from Variable
        transcription = Variable.get(f"twilio_transcription_{call_id}", default_var="No transcription available")
        logger.info(f"Transcription for call_id={call_id}: {transcription}")

        # Get recording file path from Variable
        recording_file = Variable.get(f"twilio_recording_file_{call_id}", default_var=None)
        logger.info(f"Recording file for call_id={call_id}: {recording_file}")

        # Map Twilio status to API-compatible status
        reminder_status = {
            "completed": "CallCompleted",
            "no-answer": "CallNotAnswered",
            "busy": "CallFailed",
            "failed": "CallFailed"
        }.get(twilio_status, "CallFailed")

        # For final_outcomes, use a simplified version for reporting
        final_outcomes[call_id] = {
            "completed": "CallCompleted",
            "no-answer": "CallNotAnswered",
            "busy": "CallFailed",
            "failed": "CallFailed"
        }.get(twilio_status, "CallFailed")

        loan_id = ti.xcom_pull(task_ids='generate_voice_message', key=f'loan_id_{call_id}')
        logger.info(f"Updating reminder status to {reminder_status} for call_id={call_id}, loan_id={loan_id}")
        try:
            # First PUT: Update status
            update_url = f"{api_url}loan/{loan_id}/update_reminder"
            params = {"status": reminder_status, "call_id": call_id}
            make_api_request(update_url, method="PUT", params=params)
            logger.info(f"Updated status to {reminder_status} for call_id={call_id}, loan_id={loan_id}")

            # Second PUT: Update response_text if transcription exists and is valid
            if transcription and transcription not in ["No transcription available", "Transcription failed", "Transcription unclear; review recording required"]:
                response_text = transcription[:500] if len(transcription) > 500 else transcription
                params = {"status": reminder_status, "call_id": call_id, "response_text": response_text}
                make_api_request(update_url, method="PUT", params=params)
                logger.info(f"Saved response_text for call_id={call_id}, loan_id={loan_id}: {response_text}")
        except Exception as e:
            logger.error(f"Failed to update status or response_text: {str(e)}")
            final_outcomes[call_id] = "CallFailed"

        # Analyze transcription and set new reminder if call completed
        if twilio_status == "completed" and transcription not in ["No transcription available", "Transcription failed", "Transcription unclear; review recording required"]:
            inserted_date = call_id_to_inserted_date.get(str(call_id))
            logger.info(f"Inserted date for call_id={call_id}: {inserted_date}")
            if inserted_date:
                try:
                    # Generate date from transcription
                    remind_on_date = generate_voice_message_agent(
                        loan_id=loan_id,
                        agent_url=agent_url,
                        transcription=transcription,
                        inserted_date=inserted_date
                    )
                    logger.info(f"Generated remind_on_date for call_id={call_id}: {remind_on_date}")
                    remind_on_date=extract_final_datetime(remind_on_date)
                    # Validate date format (ISO)
                    datetime.fromisoformat(remind_on_date.replace("Z", "+00:00"))

                    # Set new reminder
                    set_reminder_url = f"{api_url}loan/{loan_id}/set_reminder"
                    payload = {"remind_on": remind_on_date}
                    result = make_api_request(set_reminder_url, method="POST", json=payload)
                    new_call_id = result.get("call_id")
                    logger.info(f"Set new reminder for loan_id={loan_id}, call_id={new_call_id}, remind_on={remind_on_date}")
                except ValueError as ve:
                    logger.error(f"Invalid date format from AgentOmatic: {remind_on_date}, error: {str(ve)}")
                except Exception as e:
                    logger.error(f"Failed to set new reminder: {str(e)}")
            else:
                logger.warning(f"No inserted_date found for call_id={call_id}; skipping transcription analysis")

        # Delete Variables after processing
        Variable.delete(f"twilio_call_status_{call_id}")
        Variable.delete(f"twilio_transcription_{call_id}")
        if recording_file:
            Variable.delete(f"twilio_recording_file_{call_id}")
        logger.info(f"Deleted Variables for call_id={call_id}: twilio_call_status, twilio_transcription, twilio_recording_file")

        # Push transcription and recording file to XCom
        ti.xcom_push(key=f"transcription_{call_id}", value=transcription)
        if recording_file:
            ti.xcom_push(key=f"recording_file_{call_id}", value=recording_file)

    # Push final outcomes for reporting
    ti.xcom_push(key="final_call_outcomes", value=final_outcomes)

def update_reminder_status(**kwargs):
    """Logs the final reminder status based on call outcome and transcription using XCom data."""
    ti = kwargs['ti']
    final_call_outcomes = ti.xcom_pull(task_ids='update_call_status', key='final_call_outcomes')
    loans = ti.xcom_pull(task_ids='fetch_due_loans', key='eligible_loans')

    if not loans:
        logger.info("No eligible loans processed.")
        return

    if not final_call_outcomes:
        logger.info("No final call outcomes available.")
        for loan in loans:
            loan_id = loan['loan_id']
            logger.info(f"Call reminder status unknown for Loan ID: {loan_id}")
        return

    call_id_to_loan_id = {
        ti.xcom_pull(task_ids='generate_voice_message', key=f'call_id_{call_id}'): 
        ti.xcom_pull(task_ids='generate_voice_message', key=f'loan_id_{call_id}')
        for call_id in final_call_outcomes.keys()
    }

    for loan in loans:
        loan_id = loan['loan_id']
        call_id = next((cid for cid, lid in call_id_to_loan_id.items() if lid == loan_id), None)
        
        if call_id and call_id in final_call_outcomes:
            outcome = final_call_outcomes[call_id]
            transcription = ti.xcom_pull(task_ids='update_call_status', key=f'transcription_{call_id}')
            recording_file = ti.xcom_pull(task_ids='update_call_status', key=f'recording_file_{call_id}')
            
            logger.info(f"Call reminder status for Loan ID: {loan_id} is {outcome}")
            if outcome == "CallCompleted":
                logger.info(f"Call reminder succeeded for Loan ID: {loan_id}")
                logger.info(f"Transcription for Loan ID: {loan_id}: {transcription}")
                if recording_file:
                    logger.info(f"Recording file for Loan ID: {loan_id}: {recording_file}")
            elif outcome == "CallNotAnswered":
                logger.info(f"Call reminder not answered for Loan ID: {loan_id}")
            else:  # CallFailed or any other status
                logger.info(f"Call reminder failed for Loan ID: {loan_id}")
        else:
            logger.info(f"Call reminder status unknown for Loan ID: {loan_id}")

with DAG(
    "autofinix_reminder",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_due_loans_task = PythonOperator(
        task_id="fetch_due_loans",
        python_callable=fetch_due_loans,
        op_kwargs={
            "api_url": AUTOFINIX_API_URL,
            "test_phone_number": AUTOFINIX_TEST_PHONE_NUMBER,
            "odd_phone_number":AUTOFINIX_DEMO_PHONE_ODD,
            "even_phone_number":AUTOFINIX_DEMO_PHONE_EVEN,
        },
    )

    evaluate_due_loans_results = BranchPythonOperator(
        task_id="evaluate_due_loans_result",
        python_callable=evaluate_due_loans_result,
    )

    handle_no_due_loans = DummyOperator(
        task_id="handle_no_due_loans",
    )

    generate_voice_message_task = PythonOperator(
        task_id="generate_voice_message",
        python_callable=generate_voice_message,
        op_kwargs={
            "api_url": AUTOFINIX_API_URL,
            "agent_url": AGENTOMATIC_API_URL,
        },
    )

    trigger_send_voice_message = PythonOperator(
        task_id="trigger_twilio_voice_call",
        python_callable=trigger_twilio_voice_call,
    )

    update_call_status_task = PythonOperator(
        task_id="update_call_status",
        python_callable=update_call_status,
        op_kwargs={
            "api_url": AUTOFINIX_API_URL,
            "agent_url": AGENTOMATIC_API_URL,
        },
    )

    update_reminder_status_task = PythonOperator(
        task_id="update_reminder_status",
        python_callable=update_reminder_status,
    )

    end_task = DummyOperator(
        task_id="end_task",
        trigger_rule="all_done",
    )

    # Task Dependencies
    fetch_due_loans_task >> evaluate_due_loans_results
    evaluate_due_loans_results >> [generate_voice_message_task, handle_no_due_loans]
    generate_voice_message_task >> trigger_send_voice_message
    trigger_send_voice_message >> update_call_status_task
    update_call_status_task >> update_reminder_status_task
    update_reminder_status_task >> end_task
    handle_no_due_loans >> end_task