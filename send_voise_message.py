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

# Twilio Credentials
TWILIO_ACCOUNT_SID = Variable.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = Variable.get("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = Variable.get("TWILIO_PHONE_NUMBER")

if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
    raise ValueError("Twilio credentials are missing. Set them in Airflow Variables.")

# Base directory for storing recordings
BASE_STORAGE_DIR = "/appz/data"

with DAG(
    "send-voice-message",
    default_args=default_args,
    schedule_interval=None,  # Manually triggered
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "phone_number": Param("+1234567890", type="string", title="Phone Number"),
        "message": Param("Hello, please leave a message after the beep.", type="string", title="Message Before Beep"),
        "need_ack": Param(False, type="boolean", title="Require Acknowledgment"),
    }
) as dag:

    def initiate_call(**kwargs):
        """
        Initiate the voice call using Twilio API.
        """
        ti = kwargs["ti"]
        conf = kwargs["params"]
        logger.info(f"Received conf: {conf}")

        # Validate mandatory parameters
        if not conf.get("phone_number") or not conf.get("message"):
            logger.error("Missing required parameters: phone_number or message")
            ti.xcom_push(key="call_outcome", value="Failed")
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
            ti.xcom_push(key="call_sid", value=call.sid)
            ti.xcom_push(key="need_ack", value=need_ack)
        except Exception as e:
            logger.error(f"Failed to initiate call: {str(e)}")
            ti.xcom_push(key="call_outcome", value="Failed")
            raise

    def check_call_status(**kwargs):
        """
        Poll Twilio for call status; raise if still in-progress.
        If final, push to XCom as "call_status". Also store in Variable if call_id is provided.
        """
        ti = kwargs["ti"]
        conf = kwargs["params"]

        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        call_id = conf.get("call_id")  # Might be None if triggered manually

        if not call_sid:
            raise ValueError("No call_sid found. Did 'initiate_call' fail?")

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        call = client.calls(call_sid).fetch()
        current_status = call.status  # e.g. "completed", "no-answer", "busy", "failed", "in-progress"
        logger.info(f"Call SID={call_sid}, call_id={call_id}, status={current_status}")

        if current_status in ["completed", "no-answer", "busy", "failed"]:
            # 1) Store final status in XCom
            ti.xcom_push(key="call_status", value=current_status)

            # 2) If call_id is present, store cross-DAG data in a variable
            if call_id:
                logger.info(f"Storing Twilio final status in Variable for cross-DAG usage: twilio_call_status_{call_id}")
                Variable.set(f"twilio_call_status_{call_id}", current_status)

            return current_status
        else:
            # Force a retry
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
        If call_id is present, we can do cross-DAG references or logging, but no error if missing.
        """
        ti = kwargs["ti"]
        conf = kwargs["params"]

        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        final_status = ti.xcom_pull(task_ids="check_call_status", key="call_status")
        need_ack = ti.xcom_pull(task_ids="initiate_call", key="need_ack")
        call_id = conf.get("call_id")  # Might be None

        logger.info(f"fetch_and_save_recording with call_sid={call_sid}, status={final_status}, call_id={call_id}")

        if not need_ack:
            logger.info("need_ack=False, skipping recording download.")
            ti.xcom_push(key="recording_status", value="No Recording Needed")
            return {"message": "Recording not needed."}

        if final_status != "completed":
            logger.info(f"Cannot fetch recording because final_status={final_status}")
            ti.xcom_push(key="recording_status", value="Recording Unavailable")
            return {"message": f"Status={final_status}, no recording."}

        # Attempt to fetch
        try:
            client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
            recordings = client.recordings.list(call_sid=call_sid)

            if not recordings:
                logger.info("Recording not found. Try again later.")
                ti.xcom_push(key="recording_status", value="Recording Not Found")
                return {"message": "Recording not found yet."}

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
                logger.error(f"Failed to download recording. status code={response.status_code}")
                ti.xcom_push(key="recording_status", value="Recording Failed")
                return {"message": f"Failed to download. code={response.status_code}"}
        except Exception as e:
            logger.error(f"Recording fetch error: {str(e)}")
            ti.xcom_push(key="recording_status", value="Failed")
            return {"message": f"Exception: {str(e)}"}

    # ======================
    # Define the Operators
    # ======================

    initiate_call_task = PythonOperator(
        task_id="initiate_call",
        python_callable=initiate_call,
        provide_context=True  # In Airflow 2+, better to use op_kwargs if you prefer
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
        task_id="branch_recording_logic",
        python_callable=branch_recording_logic,
        provide_context=True
    )

    skip_recording = DummyOperator(task_id="skip_recording")

    fetch_recording_task = PythonOperator(
        task_id="fetch_and_save_recording",
        python_callable=fetch_and_save_recording,
        provide_context=True
    )

    # =======================
    # Task Dependencies
    # =======================
    initiate_call_task >> wait_task >> check_status_task
    check_status_task >> branch_task
    branch_task >> [fetch_recording_task, skip_recording]
