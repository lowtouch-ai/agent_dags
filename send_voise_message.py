from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import os
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from twilio.rest import Client

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TWILIO_ACCOUNT_SID = Variable.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = Variable.get("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = Variable.get("TWILIO_PHONE_NUMBER")

if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
    raise ValueError("Twilio credentials are missing. Set them in Airflow Variables.")

BASE_STORAGE_DIR = "/appz/data"

class TwilioVoiceTranscriber:
    """A class to handle Twilio voice calls with recording and transcription of the same audio."""

    def __init__(self, account_sid, auth_token, twilio_phone_number, base_storage_dir="/appz/data"):
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

    def initiate_call(self, phone_number, message, need_ack=True, call_id=None, ti=None):
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
            ti.xcom_push(key="call_sid", value=call.sid)
            ti.xcom_push(key="need_ack", value=need_ack)
            if call_id:
                ti.xcom_push(key="call_id", value=call_id)
        except Exception as e:
            logger.error(f"Failed to initiate call: {str(e)}")
            ti.xcom_push(key="call_outcome", value="Failed")
            raise

    def check_call_status(self, call_sid, call_id=None, need_ack=True, max_attempts=50, delay_seconds=5):
        import time
        if not call_sid:
            raise ValueError("Missing call_sid.")
        for attempt in range(max_attempts):
            call = self.client.calls(call_sid).fetch()
            current_status = call.status
            logger.info(f"Call SID={call_sid}, call ID={call_id}, status={current_status}")
            if current_status in ["completed", "no-answer", "busy", "failed"]:
                return current_status
            else:
                logger.info(f"Call still in progress, attempt {attempt + 1}/{max_attempts}")
                time.sleep(delay_seconds)
        raise AirflowException(f"Call did not complete after {max_attempts} attempts.")

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
        dag_name = "send-voice-message-1"
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
            for _ in range(10):  # Wait up to 50 seconds
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

with DAG(
    "send-voice-message-1",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "phone_number": Param("+1234567890", type="string", title="Phone Number"),
        "message": Param("Hello, please leave a message after the beep.", type="string"),
        "need_ack": Param(False, type="boolean", title="Require Acknowledgment"),
        "call_id": Param(None, type=["string", "null"], title="Call ID"),  # Added call_id to params
    }
) as dag:

    transcriber = TwilioVoiceTranscriber(
        account_sid=TWILIO_ACCOUNT_SID,
        auth_token=TWILIO_AUTH_TOKEN,
        twilio_phone_number=TWILIO_PHONE_NUMBER,
        base_storage_dir=BASE_STORAGE_DIR
    )

    def initiate_call_task_func(**kwargs):
        ti = kwargs["ti"]
        params = kwargs["params"]
        transcriber.initiate_call(
            phone_number=params["phone_number"],
            message=params["message"],
            need_ack=params["need_ack"],
            call_id=params.get("call_id"),
            ti=ti
        )

    def check_call_status_task_func(**kwargs):
        ti = kwargs["ti"]
        conf = kwargs["params"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        call_id = conf.get("call_id")
        need_ack = ti.xcom_pull(task_ids="initiate_call", key="need_ack")

        if not call_sid:
            raise ValueError("Missing call_sid. Did 'initiate_call' run?")
        
        status = transcriber.check_call_status(call_sid, call_id, need_ack)
        if call_id:
            Variable.set(f"twilio_call_status_{call_id}", status)
            ti.xcom_push(key=f"call_status_{call_id}", value=status)
        ti.xcom_push(key="call_status_final", value=status)
        return status

    def branch_based_on_status(**kwargs):
        ti = kwargs["ti"]
        final_status = ti.xcom_pull(task_ids="check_call_status", key="call_status_final")
        return "fetch_recording_task" if final_status == "completed" else "skip_recording"

    def fetch_recording_task_func(**kwargs):
        ti = kwargs["ti"]
        conf = kwargs["params"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        call_id = conf.get("call_id")
        need_ack = ti.xcom_pull(task_ids="initiate_call", key="need_ack")

        result = transcriber.fetch_and_save_recording(call_sid, call_id, need_ack)
        if call_id and "transcription" in result and result["transcription"] not in ["No transcription available", "Transcription failed", "Transcription pending"]:
            Variable.set(f"twilio_transcription_{call_id}", result["transcription"])
            logger.info(f"Set Variable twilio_transcription_{call_id} to: {result['transcription']}")
        ti.xcom_push(key="recording_status", value="Recording Saved" if result["message"].startswith("Recording downloaded") else "Recording Failed")
        return result

    # Define Operators
    initiate_call_task = PythonOperator(
        task_id="initiate_call",
        python_callable=initiate_call_task_func,
    )

    wait_task = TimeDeltaSensor(
        task_id="wait_for_call_status",
        delta=timedelta(seconds=30),
        poke_interval=10,
        mode="poke"
    )

    check_status_task = PythonOperator(
        task_id="check_call_status",
        python_callable=check_call_status_task_func,
        retries=50,
        retry_delay=timedelta(seconds=5)
    )

    branch_task = BranchPythonOperator(
        task_id="branch_based_on_status",
        python_callable=branch_based_on_status,
    )

    skip_recording = DummyOperator(task_id="skip_recording")

    fetch_recording_task = PythonOperator(
        task_id="fetch_recording_task",
        python_callable=fetch_recording_task_func,
    )

    # Set Dependencies
    initiate_call_task >> wait_task >> check_status_task
    check_status_task >> branch_task
    branch_task >> [fetch_recording_task, skip_recording]