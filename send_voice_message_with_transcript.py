from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
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

# Import the TwilioVoiceTranscriber class
import os
from twilio.rest import Client

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
TWILIO_ACCOUNT_SID = Variable.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = Variable.get("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = Variable.get("TWILIO_PHONE_NUMBER")

if not AUTOFINIX_API_URL:
    raise ValueError("Autoloan API URL is missing. Set it in Airflow Variables.")
if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
    raise ValueError("Twilio credentials are missing. Set them in Airflow Variables.")

# TwilioVoiceTranscriber class (copied inline for simplicity; could be imported from a separate file)
class TwilioVoiceTranscriber:
    def __init__(self, account_sid, auth_token, twilio_phone_number, base_storage_dir="/recordings"):
        self.client = Client(account_sid, auth_token)
        self.twilio_phone_number = twilio_phone_number
        self.base_storage_dir = base_storage_dir
        self.auth = (account_sid, auth_token)

    def make_api_request(self, url, method="GET", auth=None, retries=3):
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
            response = session.get(url, auth=auth or self.auth, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise

    def initiate_call(self, phone_number, message, need_ack=True, call_id=None):
        logger.info(f"Initiating call to: {phone_number}, need_ack: {need_ack}, call_id: {call_id}")
        try:
            if need_ack:
                twiml = f"""
                <Response>
                    <Say>{message}</Say>
                    <Say>Please speak your acknowledgment after the beep.</Say>
                    <Record maxLength="30" playBeep="true" trim="trim-silence" transcribe="true"/>
                    <Say>Thank you! Goodbye.</Say>
                </Response>
                """
            else:
                twiml = f"""
                <Response>
                    <Say>{message}</Say>
                    <Say>Thank you! Goodbye.</Say>
                </Response>
                """
            call = self.client.calls.create(
                to=phone_number,
                from_=self.twilio_phone_number,
                twiml=twiml,
            )
            logger.info(f"Call initiated with SID: {call.sid}")
            return {"call_sid": call.sid, "need_ack": need_ack, "call_id": call_id}
        except Exception as e:
            logger.error(f"Failed to initiate call: {str(e)}")
            return {"call_sid": None, "error": str(e)}

    def check_call_status(self, call_sid, call_id=None, need_ack=True, max_attempts=50, delay_seconds=5):
        import time
        if not call_sid:
            raise ValueError("Missing call_sid.")
        for attempt in range(max_attempts):
            call = self.client.calls(call_sid).fetch()
            current_status = call.status
            logger.info(f"Call SID={call_sid}, call ID={call_id}, status={current_status}")
            if current_status in ["completed", "no-answer", "busy", "failed"]:
                result = {"status": current_status}
                if call_id:
                    result["call_id"] = call_id
                return result
            else:
                logger.info(f"Call still in progress, attempt {attempt + 1}/{max_attempts}")
                time.sleep(delay_seconds)
        raise Exception(f"Call did not complete after {max_attempts} attempts.")

    def fetch_and_save_recording(self, call_sid, call_id=None, need_ack=True):
        if not need_ack:
            logger.info("need_ack is False, no recording or transcription to process.")
            return {"message": "No acknowledgment required."}
        recordings = self.client.recordings.list(call_sid=call_sid)
        if not recordings:
            logger.info("Recording not found yet.")
            return {"message": "Recording not available yet.", "transcription": "No transcription available"}
        recording_sid = recordings[0].sid
        recording_url = f"https://api.twilio.com{recordings[0].uri.replace('.json', '.mp3')}"
        execution_date = datetime.now().strftime("%Y-%m-%d")
        dag_name = "send-voice-message-with-transcription"
        save_directory = os.path.join(self.base_storage_dir, dag_name, execution_date)
        os.makedirs(save_directory, exist_ok=True)
        file_path = os.path.join(save_directory, f"{call_sid}.mp3")
        try:
            response = self.make_api_request(recording_url)
            with open(file_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Recording saved at {file_path}")
        except Exception as e:
            logger.error(f"Failed to download recording: {str(e)}")
            return {"message": "Failed to download recording", "transcription": "No transcription available"}
        import time
        transcriptions = self.client.recordings(recording_sid).transcriptions.list()
        if transcriptions:
            transcription = transcriptions[0]
            for _ in range(10):
                transcription = self.client.transcriptions(transcription.sid).fetch()
                if transcription.status == "completed":
                    transcription_text = transcription.transcription_text
                    logger.info(f"Transcribed text for call SID={call_sid}: {transcription_text}")
                    return {
                        "message": "Recording downloaded and transcribed successfully",
                        "file_path": file_path,
                        "transcription": transcription_text
                    }
                elif transcription.status == "failed":
                    logger.error(f"Transcription failed for call SID={call_sid}")
                    return {
                        "message": "Recording downloaded but transcription failed",
                        "file_path": file_path,
                        "transcription": "Transcription failed"
                    }
                time.sleep(5)
            logger.info(f"Transcription not completed in time for call SID={call_sid}")
            return {
                "message": "Recording downloaded but transcription not completed",
                "file_path": file_path,
                "transcription": "Transcription pending"
            }
        else:
            logger.info(f"No transcription available yet for call SID={call_sid}")
            return {
                "message": "Recording downloaded but no transcription available",
                "file_path": file_path,
                "transcription": "No transcription available"
            }

# Existing helper functions
def make_api_request(url, method="GET", params=None, json=None, retries=3):
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
        else:
            raise ValueError(f"Unsupported method: {method}")
        response.raise_for_status()
        return response.json() if response.content else {}
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

def fetch_due_loans(api_url, test_phone_number, **kwargs):
    ti = kwargs['ti']
    try:
        url = f"{api_url}loan/get_reminder?status=Reminder"
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
                reminder["phone"] = test_phone_number
                logger.info(f"Updated reminder with phone number: {reminder}")
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
    ti = kwargs['ti']
    loans = ti.xcom_pull(task_ids='fetch_due_loans', key='eligible_loans')
    if loans and isinstance(loans, list) and len(loans) > 0:
        return "generate_voice_message"
    else:
        return "handle_no_due_loans"

def generate_voice_message_agent(loan_id, agent_url):
    client = Client(
        host=agent_url,
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
                    "The final response must be a concise message which should not exceed 500 characters, containing only relevant content, "
                    "suitable for conversion to a voice call."
                )
            }
        ],
        stream=False
    )
    agent_response = response['message']['content']
    logging.info(f"AgentOmatic Response: {agent_response}")
    return agent_response

def clean_message(message):
    return re.sub(r'[\n\t]', ' ', message).strip()

def generate_voice_message(api_url, agent_url, **kwargs):
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
    """Trigger Twilio voice call using TwilioVoiceTranscriber and save recording/transcription."""
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

    transcriber = TwilioVoiceTranscriber(
        account_sid=TWILIO_ACCOUNT_SID,
        auth_token=TWILIO_AUTH_TOKEN,
        twilio_phone_number=TWILIO_PHONE_NUMBER
    )
    final_outcomes = {}

    for call_id in call_ids:
        conf = ti.xcom_pull(task_ids="generate_voice_message", key=f"voice_message_payload_{call_id}")
        if not conf:
            logger.info(f"No voice message payload available for call_id {call_id}, skipping")
            final_outcomes[call_id] = "CallFailed"
            continue

        # Initiate and process the call
        result = transcriber.process_call(
            phone_number=conf["phone_number"],
            message=conf["message"],
            need_ack=conf["need_ack"],
            call_id=call_id
        )

        if result.get("call_sid"):
            call_sid = result["call_sid"]
            status = result.get("status", "failed")
            transcription = result.get("transcription", "No transcription available")
            file_path = result.get("file_path")

            # Set Twilio status in Variable
            Variable.set(f"twilio_call_status_{call_id}", status)
            # Set transcription in Variable
            Variable.set(f"twilio_transcription_{call_id}", transcription)
            # Store recording file path in XCom (optional)
            ti.xcom_push(key=f"recording_file_{call_id}", file_path)

            # Map status for final outcomes
            final_outcomes[call_id] = {
                "completed": "CallCompleted",
                "no-answer": "CallNotAnswered",
                "busy": "CallFailed",
                "failed": "CallFailed"
            }.get(status, "CallFailed")
            logger.info(f"Call ID: {call_id}, Status: {status}, Transcription: {transcription}, Recording: {file_path}")
        else:
            logger.error(f"Call failed for call_id {call_id}: {result.get('error')}")
            final_outcomes[call_id] = "CallFailed"
            Variable.set(f"twilio_call_status_{call_id}", "failed")
            Variable.set(f"twilio_transcription_{call_id}", "Call failed, no transcription")

    ti.xcom_push(key="final_call_outcomes", value=final_outcomes)

def update_call_status(api_url, **kwargs):
    ti = kwargs['ti']
    call_ids = ti.xcom_pull(task_ids='generate_voice_message', key='call_ids') or []
    final_outcomes = ti.xcom_pull(task_ids='trigger_twilio_voice_call', key='final_call_outcomes') or {}

    for call_id in call_ids:
        twilio_status = Variable.get(f"twilio_call_status_{call_id}", default_var="failed")
        transcription = Variable.get(f"twilio_transcription_{call_id}", default_var="No transcription")
        logger.info(f"Twilio status for call_id={call_id}: {twilio_status}, Transcription: {transcription}")

        reminder_status = {
            "completed": "CalledCompleted",
            "no-answer": "CallNotAnswered",
            "busy": "CallFailed",
            "failed": "CallFailed"
        }.get(twilio_status, "CallFailed")

        loan_id = ti.xcom_pull(task_ids='generate_voice_message', key=f'loan_id_{call_id}')
        logger.info(f"Updating reminder status to {reminder_status} for call_id={call_id}, loan_id={loan_id}")
        try:
            update_url = f"{api_url}loan/{loan_id}/update_reminder"
            params = {"status": reminder_status, "call_id": call_id}
            make_api_request(update_url, method="PUT", params=params)
            logger.info(f"Updated status to {reminder_status} for call_id={call_id}, loan_id={loan_id}")
            final_outcomes[call_id] = reminder_status.replace("ed", "e")  # e.g., "CalledCompleted" -> "CallComplete"
            Variable.delete(f"twilio_call_status_{call_id}")
            # Optionally keep transcription Variable or delete it
            # Variable.delete(f"twilio_transcription_{call_id}")
            logger.info(f"Deleted Variable key twilio_call_status_{call_id}")
        except Exception as e:
            logger.error(f"Failed to update status: {str(e)}")
            final_outcomes[call_id] = "CallFailed"

    ti.xcom_push(key="final_call_outcomes", value=final_outcomes)

def update_reminder_status(**kwargs):
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
            transcription = Variable.get(f"twilio_transcription_{call_id}", default_var="No transcription")
            logger.info(f"Call reminder status for Loan ID: {loan_id} is {outcome}, Transcription: {transcription}")
            if outcome == "CallComplete":  # Adjusted for simplified status
                logger.info(f"Call reminder succeeded for Loan ID: {loan_id}")
            elif outcome == "CallNotAnswered":
                logger.info(f"Call reminder not answered for Loan ID: {loan_id}")
            else:
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
        op_kwargs={"api_url": AUTOFINIX_API_URL, "test_phone_number": AUTOFINIX_TEST_PHONE_NUMBER},
    )

    evaluate_due_loans_results = BranchPythonOperator(
        task_id="evaluate_due_loans_result",
        python_callable=evaluate_due_loans_result,
    )

    handle_no_due_loans = DummyOperator(task_id="handle_no_due_loans")

    generate_voice_message_task = PythonOperator(
        task_id="generate_voice_message",
        python_callable=generate_voice_message,
        op_kwargs={"api_url": AUTOFINIX_API_URL, "agent_url": AGENTOMATIC_API_URL},
    )

    trigger_send_voice_message = PythonOperator(
        task_id="trigger_twilio_voice_call",
        python_callable=trigger_twilio_voice_call,
    )

    update_call_status_task = PythonOperator(
        task_id="update_call_status",
        python_callable=update_call_status,
        op_kwargs={"api_url": AUTOFINIX_API_URL},
    )

    update_reminder_status_task = PythonOperator(
        task_id="update_reminder_status",
        python_callable=update_reminder_status,
    )

    end_task = DummyOperator(task_id="end_task", trigger_rule="all_done")

    fetch_due_loans_task >> evaluate_due_loans_results
    evaluate_due_loans_results >> [generate_voice_message_task, handle_no_due_loans]
    generate_voice_message_task >> trigger_send_voice_message
    trigger_send_voice_message >> update_call_status_task
    update_call_status_task >> update_reminder_status_task
    update_reminder_status_task >> end_task
    handle_no_due_loans >> end_task