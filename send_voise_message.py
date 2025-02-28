from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from twilio.rest import Client
import requests
import os
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Default args for DAG
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

# Ensure Twilio credentials are set
if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
    raise ValueError("Twilio credentials are missing. Set them in Airflow Variables.")

# Define base directory for storing recordings inside the container
BASE_STORAGE_DIR = "/appz/data"

# Define DAG
with DAG(
    "twilio_voice_call_direct",
    default_args=default_args,
    schedule_interval=None,  # Manually triggered
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "phone_number": Param("+1234567890", type="string", title="Phone Number", description="Recipient's phone number."),
        "message": Param("Hello, please leave a message after the beep.", type="string", title="Message Before Beep"),
        "need_ack": Param(False, type="boolean", title="Require Acknowledgment", description="Set to true if you need an acknowledgment."),
    }
) as dag:

    def initiate_call(**kwargs):
        """Initiate the voice call using Twilio API"""
        conf = kwargs["params"]
        phone_number = conf["phone_number"]
        message = conf["message"]
        need_ack = conf["need_ack"]

        logger.info(f"Initiating call to: {phone_number}, need_ack: {need_ack}")

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
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
        kwargs["ti"].xcom_push(key="need_ack", value=need_ack)  # Store need_ack for later use

    def check_call_status(**kwargs):
        """Check call status via Twilio API"""
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        ti = kwargs["ti"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")

        logger.info(f"Checking call status for SID: {call_sid}")

        call = client.calls(call_sid).fetch()
        logger.info(f"Call status: {call.status}")

        if call.status in ["completed", "no-answer", "busy", "failed"]:
            ti.xcom_push(key="call_status", value=call.status)
            return call.status  # Return status instead of raising an error

        raise ValueError(f"Call not yet completed: {status}")

    def fetch_and_save_recording(**kwargs):
        """Fetch and save the call recording if `need_ack` is True"""
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        ti = kwargs["ti"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        call_status = ti.xcom_pull(task_ids="check_call_status")
        need_ack = ti.xcom_pull(task_ids="initiate_call", key="need_ack")

        logger.info(f"Checking if recording is available for call SID: {call_sid}")

        # If `need_ack` is False, skip saving the recording
        if not need_ack:
            logger.info("need_ack is False, skipping recording download.")
            ti.xcom_push(key="recording_status", value="No Recording Needed")
            return {"message": "Recording not needed as acknowledgment is not required."}

        # Only fetch recording if the call was completed
        if call_status != "completed":
            logger.info(f"Recording unavailable. Call status: {call_status}")
            ti.xcom_push(key="recording_status", value="Recording Unavailable")
            return {"message": f"Cannot fetch recording. Call status: {call_status}"}

        # Get the recording list for the call
        recordings = client.recordings.list(call_sid=call_sid)

        if not recordings:
            logger.info("Recording not found yet. Try again later.")
            ti.xcom_push(key="recording_status", value="Recording Not Found")
            return {"message": "Recording not available yet. Try again later."}

        # Get the most recent recording URL
        recording_url = f"https://api.twilio.com{recordings[0].uri.replace('.json', '.mp3')}"

        # Generate dynamic file path based on DAG name and execution date
        execution_date = datetime.now().strftime("%Y-%m-%d")
        dag_name = "twilio_voice_call_direct"
        save_directory = os.path.join(BASE_STORAGE_DIR, dag_name, execution_date)
        os.makedirs(save_directory, exist_ok=True)  # Ensure directory exists

        # Save file inside container
        file_path = os.path.join(save_directory, f"{call_sid}.mp3")

        # Download the MP3 file
        response = requests.get(recording_url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))

        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)

            logger.info(f"Recording saved at {file_path}")
            ti.xcom_push(key="recording_status", value="Recording Saved")
            return {"message": "Recording downloaded successfully", "file_path": file_path}

        ti.xcom_push(key="recording_status", value="Recording Failed")
        return {"message": "Failed to download recording"}


    # Define tasks
    initiate_call_task = PythonOperator(
        task_id="initiate_call",
        python_callable=initiate_call,
        provide_context=True
    )

    wait_task = TimeDeltaSensor(
        task_id="wait_for_call_status",
        delta=timedelta(seconds=5),  # Wait for 5 seconds
        poke_interval=5,
        mode="poke"
    )

    check_status_task = PythonOperator(
        task_id="check_call_status",
        python_callable=check_call_status,
        provide_context=True,
        retries=50,  # Keeps checking until status is completed or no-answer
        retry_delay=timedelta(seconds=5)
    )

    fetch_recording_task = PythonOperator(
        task_id="fetch_and_save_recording",
        python_callable=fetch_and_save_recording,
        provide_context=True
    )

    # Set task dependencies
    initiate_call_task >> wait_task >> check_status_task
    check_status_task >> fetch_recording_task  # Only runs if call is completed