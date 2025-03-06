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
        else:
            raise ValueError(f"Unsupported method: {method}")

        response.raise_for_status()
        return response.json() if response.content else {}
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

def fetch_due_loans(api_url, test_phone_number, **kwargs):
    """Fetches loans that are due from the Autoloan API"""
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

# ... (evaluate_due_loans_result and generate_voice_message_agent remain unchanged)

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

def update_call_status(api_url, **kwargs):
    """Updates call status after Twilio DAG completion."""
    ti = kwargs['ti']
    call_ids = ti.xcom_pull(task_ids='generate_voice_message', key='call_ids') or []

    for call_id in call_ids:
        twilio_status = Variable.get(f"twilio_call_status_{call_id}", default_var=None)
        if not twilio_status:
            twilio_status = "failed"
        logger.info(f"Twilio status for call_id={call_id}: {twilio_status}")

        reminder_status = {
            "completed": "CallCompleted",
            "no-answer": "CallNotAnswered",
            "busy": "CallFailed",
            "failed": "CallFailed"
        }.get(twilio_status, "Unknown")

        loan_id = ti.xcom_pull(task_ids='generate_voice_message', key=f'loan_id_{call_id}')
        logger.info(f"Updating reminder status to {reminder_status} for call_id={call_id}, loan_id={loan_id}")
        try:
            update_url = f"{api_url}loan/{loan_id}/update_reminder"
            params = {"status": reminder_status, "call_id": call_id}
            make_api_request(update_url, method="PUT", params=params)
            logger.info(f"Updated status to {reminder_status} for call_id={call_id}, loan_id={loan_id}")
            
            Variable.delete(f"twilio_call_status_{call_id}")
            logger.info(f"Deleted Variable key twilio_call_status_{call_id}")
        except Exception as e:
            logger.error(f"Failed to update status: {str(e)}")

# ... (rest of the DAG definition remains unchanged)