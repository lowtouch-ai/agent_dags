from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import datetime, timedelta
from twilio.rest import Client
import requests
import os
import logging
import uuid  # For generating a default call_id
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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

# Twilio Credentials
TWILIO_ACCOUNT_SID = Variable.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = Variable.get("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = Variable.get("TWILIO_PHONE_NUMBER")

if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
    raise ValueError("Twilio credentials are missing. Set them in Airflow Variables.")

# Base directory for storing recordings
BASE_STORAGE_DIR = "/appz/data"

def make_api_request(url, method="GET", auth=None, retries=3):
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
            response = session.get(url, auth=auth or (TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN), timeout=10)
        elif method == "POST":
            response = session.post(url, auth=auth or (TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN), timeout=10)
        else:
            raise ValueError(f"Unsupported method: {method}")

        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

with DAG(
    "send-voice-message-transcript",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "phone_number": Param("+1234567890", type="string", title="Phone Number"),
        "message": Param("Hello, please leave a message after the beep.", type="string", title="Message Before Beep"),
        "need_ack": Param(False, type="boolean", title="Require Acknowledgment"),
        "call_id": Param(None, type=["string", "null"], title="Call ID"),
    }
) as dag:

    def initiate_call(**kwargs):
        """
        Initiate the voice call using Twilio API with recording and transcription.
        """
        ti = kwargs["ti"]
        conf = kwargs["params"]
        logger.info(f"Received conf: {conf}")

        if not conf.get("phone_number") or not conf.get("message"):
            logger.error("Missing required parameters: phone_number or message")
            ti.xcom_push(key="call_outcome", value="Failed")
            raise ValueError("Missing required parameters: phone_number or message")

        phone_number = conf["phone_number"]
        message = conf["message"]
        need_ack = conf.get("need_ack", False)
        call_id = conf.get("call_id", str(uuid.uuid4()))  # Default call_id if not provided

        logger.info(f"Initiating call to: {phone_number}, need_ack: {need_ack}, call_id: {call_id}")

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
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
            call = client.calls.create(
                to=phone_number,
                from_=TWILIO_PHONE_NUMBER,
                twiml=twiml,
            )
            logger.info(f"Call initiated with SID: {call.sid}")
            ti.xcom_push(key="call_sid", value=call.sid)
            ti.xcom_push(key="need_ack", value=need_ack)
            ti.xcom_push(key="call_id", value=call_id)  # Always push call_id
        except Exception as e:
            logger.error(f"Failed to initiate call: {str(e)}")
            ti.xcom_push(key="call_outcome", value="Failed")
            raise

    def check_call_status(**kwargs):
        """
        Poll Twilio for call status; raise if still in-progress.
        """
        ti = kwargs["ti"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        call_id = ti.xcom_pull(task_ids="initiate_call", key="call_id")

        if not call_sid:
            raise ValueError("No call_sid found. Did 'initiate_call' fail?")

        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{call_sid}.json"
        response = make_api_request(url)
        current_status = response.json()["status"]
        logger.info(f"Call SID={call_sid}, call_id={call_id}, status={current_status}")

        if current_status in ["completed", "no-answer", "busy", "failed"]:
            ti.xcom_push(key="call_status", value=current_status)
            if call_id:
                logger.info(f"Storing Twilio final status in Variable for cross-DAG usage: twilio_call_status_{call_id}")
                Variable.set(f"twilio_call_status_{call_id}", current_status)
            return current_status
        else:
            raise AirflowException(f"Call is still {current_status}. Retrying...")

    def branch_recording_logic(**kwargs):
        """
        If final status is 'completed', proceed to 'fetch_and_save_recording'.
        Otherwise skip.
        """
        ti = kwargs["ti"]
        final_status = ti.xcom_pull(task_ids="check_call_status", key="call_status")

        logger.info(f"branch_recording_logic sees final_status={final_status}")
        if final_status == "completed":
            return "fetch_and_save_recording"
        else:
            return "skip_recording"

    def fetch_and_save_recording(**kwargs):
        """
        Fetch and save the recording if need_ack=True and call_status='completed'.
        """
        ti = kwargs["ti"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        final_status = ti.xcom_pull(task_ids="check_call_status", key="call_status")
        need_ack = ti.xcom_pull(task_ids="initiate_call", key="need_ack")
        call_id = ti.xcom_pull(task_ids="initiate_call", key="call_id")

        logger.info(f"fetch_and_save_recording with call_sid={call_sid}, status={final_status}, call_id={call_id}")

        if not need_ack:
            logger.info("need_ack=False, skipping recording download.")
            ti.xcom_push(key="recording_status", value="No Recording Needed")
            return {"message": "Recording not needed."}

        if final_status != "completed":
            logger.info(f"Cannot fetch recording because final_status={final_status}")
            ti.xcom_push(key="recording_status", value="Recording Unavailable")
            return {"message": f"Status={final_status}, no recording."}

        try:
            url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Recordings.json?CallSid={call_sid}"
            response = make_api_request(url)
            recordings = response.json().get("recordings", [])

            if not recordings:
                logger.info("Recording not found yet.")
                ti.xcom_push(key="recording_status", value="Recording Not Found")
                raise AirflowException("Recording not found yet.")

            recording_sid = recordings[0]["sid"]
            recording_url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Recordings/{recording_sid}.mp3"
            execution_date = datetime.now().strftime("%Y-%m-%d")
            dag_name = "send-voice-message-1"
            save_directory = os.path.join(BASE_STORAGE_DIR, dag_name, execution_date)
            os.makedirs(save_directory, exist_ok=True)
            file_path = os.path.join(save_directory, f"{call_sid}.mp3")

            response = make_api_request(recording_url)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(response.content)
                logger.info(f"Recording saved at {file_path}")
                ti.xcom_push(key="recording_status", value="Recording Saved")
                ti.xcom_push(key="recording_sid", value=recording_sid)
                if call_id:
                    Variable.set(f"twilio_recording_file_{call_id}", file_path)
                    logger.info(f"Set Variable twilio_recording_file_{call_id} to: {file_path}")
                    ti.xcom_push(key=f"recording_file_{call_id}", value=file_path)
                return {"message": "Recording downloaded successfully", "file_path": file_path}
            else:
                logger.error(f"Failed to download recording. status code={response.status_code}")
                ti.xcom_push(key="recording_status", value="Recording Failed")
                raise AirflowException(f"Failed to download recording: {response.status_code}")

        except Exception as e:
            logger.error(f"Recording fetch error: {str(e)}")
            ti.xcom_push(key="recording_status", value="Failed")
            raise

    def trigger_transcription_dag(**kwargs):
        """
        Trigger the voice_text_transcribe DAG with the recording file path and call_id.
        """
        ti = kwargs["ti"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        call_id = ti.xcom_pull(task_ids="initiate_call", key="call_id")
        logger.info(f"Triggering transcription for call SID={call_sid}, call_id={call_id}")
        recording_file_path = ti.xcom_pull(task_ids="fetch_and_save_recording", key=f"recording_file_{call_id}")
        logger.info(f"Recording file path: {recording_file_path}")
        if not recording_file_path:
            logger.error("No recording file path found. Did 'fetch_and_save_recording' fail?")
            ti.xcom_push(key="transcription_status", value="failed")
            raise AirflowException("No recording file path available")

        logger.info(f"Triggering transcription for call SID={call_sid}, file={recording_file_path}, call_id={call_id}")

        # Trigger the voice_text_transcribe DAG
        trigger_dag = TriggerDagRunOperator(
            task_id="trigger_transcription_dag",
            trigger_dag_id="voice_text_transcribe",
            conf={
                "file_path": {
                    "value": recording_file_path
                },
                "call_id": call_id  # Pass call_id to the target DAG
            },
            dag=dag,
        )
        trigger_dag.execute(context=kwargs)

    def fetch_transcription(**kwargs):
        """
        Fetch the transcription from the Variable text_{call_id} with retry using TimeDeltaSensor.
        """
        ti = kwargs["ti"]
        call_id = ti.xcom_pull(task_ids="initiate_call", key="call_id")
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")

        variable_key = f"text_{call_id}"
        transcription = Variable.get(variable_key, default=None)

        if transcription is not None:
            logger.info(f"Transcription retrieved for call SID={call_sid}: {transcription}")
            # Delete the Variable after retrieval
            Variable.delete(variable_key)
            logger.info(f"Deleted Variable {variable_key}")
            # Re-save for consistency with original logic
            Variable.set(f"twilio_transcription_{call_id}", transcription)
            logger.info(f"Set Variable twilio_transcription_{call_id} to: {transcription}")
            ti.xcom_push(key="transcription_status", value="completed")
            return {
                "message": "Transcription fetched successfully",
                "transcription": transcription
            }
        else:
            logger.info(f"Transcription not available yet for call SID={call_sid}")
            raise AirflowException("Transcription not yet available; retrying...")

    # Define Operators
    initiate_call_task = PythonOperator(
        task_id="initiate_call",
        python_callable=initiate_call,
        provide_context=True
    )

    wait_call_status = TimeDeltaSensor(
        task_id="wait_for_call_status",
        delta=timedelta(seconds=30),
        poke_interval=10,
        mode="poke"
    )

    check_status_task = PythonOperator(
        task_id="check_call_status",
        python_callable=check_call_status,
        provide_context=True,
        retries=50,
        retry_delay=timedelta(seconds=5)
    )

    branch_recording_task = BranchPythonOperator(
        task_id="branch_recording_logic",
        python_callable=branch_recording_logic,
        provide_context=True
    )

    fetch_recording_task = PythonOperator(
        task_id="fetch_and_save_recording",
        python_callable=fetch_and_save_recording,
        provide_context=True
    )

    trigger_transcription_task = PythonOperator(
        task_id="trigger_transcription_dag",
        python_callable=trigger_transcription_dag,
        provide_context=True
    )

    # Use TimeDeltaSensor to wait before fetching transcription
    wait_for_transcription = TimeDeltaSensor(
        task_id="wait_for_transcription",
        delta=timedelta(seconds=5),  # Wait 5 seconds between retries
        poke_interval=5,
        mode="poke",
    )

    fetch_transcription_task = PythonOperator(
        task_id="fetch_transcription",
        python_callable=fetch_transcription,
        provide_context=True,
        retries=12,  # 12 retries × 5s = 60s total timeout
        retry_delay=timedelta(seconds=5)
    )

    skip_recording = DummyOperator(task_id="skip_recording")

    # Task Dependencies
    initiate_call_task >> wait_call_status >> check_status_task
    check_status_task >> branch_recording_task
    branch_recording_task >> [fetch_recording_task, skip_recording]
    fetch_recording_task >> trigger_transcription_task >> wait_for_transcription >> fetch_transcription_task