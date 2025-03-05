from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import datetime, timedelta
from twilio.rest import Client
import requests
import os
import logging

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

with DAG(
    "send-voice-message-1",
    default_args=default_args,
    schedule_interval=None,  # Manually triggered
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "phone_number": Param("+1234567890", type="string", title="Phone Number"),
        "message": Param("Hello, please leave a message after the beep.", type="string"),
        "need_ack": Param(False, type="boolean", title="Require Acknowledgment"),
    }
) as dag:

    # --------------------------------------
    # 1. Initiate the Call
    # --------------------------------------
    def initiate_call(**kwargs):
        """Initiate the voice call using Twilio API."""
        conf = kwargs["params"]
        logger.info(f"Received conf: {conf}")

        if not conf.get("phone_number") or not conf.get("message"):
            logger.error("Missing required parameters: phone_number or message")
            kwargs["ti"].xcom_push(key="call_outcome", value="Failed")
            raise ValueError("Missing required parameters: phone_number or message")

        phone_number = conf["phone_number"]
        message = conf["message"]
        need_ack = conf.get("need_ack", False)

        logger.info(f"Initiating call to: {phone_number}, need_ack: {need_ack}")

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        try:
            call = client.calls.create(
                to=phone_number,
                from_=TWILIO_PHONE_NUMBER,
                twiml=f"""
                <Response>
                    <Say>{message}, acknowledge this call after beep</Say>
                    <Record maxLength="30" playBeep="true" />
                    <Say>Thank you! Goodbye.</Say>
                </Response>
                """
            )
            logger.info(f"Call initiated with SID: {call.sid}")
            kwargs["ti"].xcom_push(key="call_sid", value=call.sid)
            kwargs["ti"].xcom_push(key="need_ack", value=need_ack)
        except Exception as e:
            logger.error(f"Failed to initiate call: {str(e)}")
            kwargs["ti"].xcom_push(key="call_outcome", value="Failed")
            raise

    # --------------------------------------
    # 2. Check Call Status (with Retries)
    # --------------------------------------
    def check_call_status(**kwargs):
        """Poll Twilio for call status, raise exception if still in-progress."""
        ti = kwargs["ti"]
        conf = kwargs["params"]

        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        # If the parent DAG passes call_id, we can store status in a variable
        # But we won't use call_id for branching logic
        call_id = conf.get("call_id", None)

        if not call_sid:
            raise ValueError("Missing call_sid. Did 'initiate_call' run?")

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        call = client.calls(call_sid).fetch()
        current_status = call.status  # "completed", "no-answer", "busy", "failed", "in-progress"
        logger.info(f"Call SID={call_sid}, call ID={call_id}, status={current_status}")

        # If final, store cross-DAG data if call_id is present
        if current_status in ["completed", "no-answer", "busy", "failed"]:
            if call_id:
                Variable.set(f"twilio_call_status_{call_id}", current_status)
                ti.xcom_push(key=f"call_status_{call_id}", value=current_status)
            # Also store a "call_status_final" for the branching logic
            ti.xcom_push(key="call_status_final", value=current_status)
            return current_status
        else:
            # Force a retry if still in-progress
            raise AirflowException(f"Call is still {current_status}. Retrying...")

    # --------------------------------------
    # 3. Branch to "fetch_recording" if COMPLETED, else skip
    # --------------------------------------
    def branch_based_on_status(**kwargs):
        ti = kwargs["ti"]
        final_status = ti.xcom_pull(task_ids="check_call_status", key="call_status_final")

        # If final_status not found, or not "completed", we skip
        if final_status == "completed":
            logger.info(f"Call status is 'completed'; proceeding to fetch the recording.")
            return "fetch_recording_task"
        else:
            logger.info(f"Call status is '{final_status}'; skipping recording.")
            return "skip_recording"

    # --------------------------------------
    # 4. Fetch and Save Recording
    # --------------------------------------
    def fetch_and_save_recording(**kwargs):
        """Only run if Twilio call was 'completed'."""
        ti = kwargs["ti"]
        conf = kwargs["params"]

        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        final_status = ti.xcom_pull(task_ids="check_call_status", key="call_status_final")
        need_ack = ti.xcom_pull(task_ids="initiate_call", key="need_ack")

        # We can do cross-DAG storage if a call_id was passed
        call_id = conf.get("call_id", None)
        logger.info(f"fetch_and_save_recording called with final_status={final_status}, call_id={call_id}")

        if not need_ack:
            logger.info("need_ack is False, skipping recording download.")
            ti.xcom_push(key="recording_status", value="No Recording Needed")
            return {"message": "No acknowledgment required."}

        # final_status should be "completed" if we're here
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        recordings = client.recordings.list(call_sid=call_sid)

        if not recordings:
            logger.info("Recording not found yet. Try again later.")
            ti.xcom_push(key="recording_status", value="Recording Not Found")
            return {"message": "Recording not available yet. Try again later."}

        recording_url = f"https://api.twilio.com{recordings[0].uri.replace('.json', '.mp3')}"
        execution_date = datetime.now().strftime("%Y-%m-%d")
        dag_name = "send-voice-message-1"
        save_directory = os.path.join(BASE_STORAGE_DIR, dag_name, execution_date)
        os.makedirs(save_directory, exist_ok=True)
        file_path = os.path.join(save_directory, f"{call_sid}.mp3")

        response = requests.get(recording_url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Recording saved at {file_path}")
            ti.xcom_push(key="recording_status", value="Recording Saved")
            return {"message": "Recording downloaded successfully", "file_path": file_path}
        else:
            logger.error(f"Failed to download recording, status code: {response.status_code}")
            ti.xcom_push(key="recording_status", value="Recording Failed")
            return {"message": "Failed to download recording"}

    # --------------------------------------
    # 5. Define Operators
    # --------------------------------------
    initiate_call_task = PythonOperator(
        task_id="initiate_call",
        python_callable=initiate_call,
        provide_context=True
    )

    wait_task = TimeDeltaSensor(
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

    branch_task = BranchPythonOperator(
        task_id="branch_based_on_status",
        python_callable=branch_based_on_status,
        provide_context=True
    )

    skip_recording = DummyOperator(task_id="skip_recording")

    fetch_recording_task = PythonOperator(
        task_id="fetch_recording_task",
        python_callable=fetch_and_save_recording,
        provide_context=True
    )

    # --------------------------------------
    # 6. Set Dependencies
    # --------------------------------------
    initiate_call_task >> wait_task >> check_status_task
    check_status_task >> branch_task
    branch_task >> [fetch_recording_task, skip_recording]
