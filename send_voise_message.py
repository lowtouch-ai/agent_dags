from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
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
    "send-voice-message-1",
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
        logger.info(f"Received conf: {conf}")

        # Validate required parameters
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

    
    def check_call_status(**kwargs):
        """Poll Twilio for call status, raise exception if still in-progress."""
        ti = kwargs["ti"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        call_id = kwargs["params"].get("call_id")  # Unique call ID passed in conf

        if not call_sid or not call_id:
            # We mark XCom as 'Failed' for completeness
            ti.xcom_push(key=f"call_status_{call_id}", value="Failed")
            raise ValueError("Missing call_sid or call_id")

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        call = client.calls(call_sid).fetch()
        current_status = call.status  # e.g., "in-progress", "completed", "no-answer", "busy", "failed"
        logger.info(f"Call SID={call_sid}, call ID={call_id}, status={current_status}")

        # If final, store in Variable and XCom
        if current_status in ["completed", "no-answer", "busy", "failed"]:
            # 1) Store final status for cross-DAG usage
            Variable.set(f"twilio_call_status_{call_id}", current_status)

            # 2) Also store in XCom for local usage
            ti.xcom_push(key=f"call_status_{call_id}", value=current_status)

            # If you'd like to return the status for referencing in the next task
            return current_status
        else:
            # If call is still in-progress, raise an exception to trigger a retry
            raise AirflowException(f"Call is still {current_status}. Retrying...")



    def fetch_and_save_recording(**kwargs):
        """Fetch and save the call recording if `need_ack` is True"""
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        ti = kwargs["ti"]
        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        call_status = ti.xcom_pull(task_ids="check_call_status")
        need_ack = ti.xcom_pull(task_ids="initiate_call", key="need_ack")
        conf = kwargs["params"]
        call_id = conf.get("call_id", "default_call_id")

        logger.info(f"Checking if recording is available for call SID: {call_sid}, call_id: {call_id}")

        if not need_ack:
            logger.info("need_ack is False, skipping recording download.")
            ti.xcom_push(key=f"recording_status_{call_id}", value="No Recording Needed")
            logger.info(f"Pushed XCom: key=recording_status_{call_id}, value=No Recording Needed")
            return {"message": "Recording not needed as acknowledgment is not required."}

        if call_status != "completed":
            logger.info(f"Recording unavailable. Call status: {call_status}")
            ti.xcom_push(key=f"recording_status_{call_id}", value="Recording Unavailable")
            logger.info(f"Pushed XCom: key=recording_status_{call_id}, value=Recording Unavailable")
            return {"message": f"Cannot fetch recording. Call status: {call_status}"}

        try:
            recordings = client.recordings.list(call_sid=call_sid)

            if not recordings:
                logger.info("Recording not found yet. Try again later.")
                ti.xcom_push(key=f"recording_status_{call_id}", value="Recording Not Found")
                logger.info(f"Pushed XCom: key=recording_status_{call_id}, value=Recording Not Found")
                return {"message": "Recording not available yet. Try again later."}

            recording_url = f"https://api.twilio.com{recordings[0].uri.replace('.json', '.mp3')}"
            execution_date = datetime.now().strftime("%Y-%m-%d")
            dag_name = "send-voice-message"
            save_directory = os.path.join(BASE_STORAGE_DIR, dag_name, execution_date)
            os.makedirs(save_directory, exist_ok=True)
            file_path = os.path.join(save_directory, f"{call_sid}.mp3")

            response = requests.get(recording_url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))

            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(response.content)
                logger.info(f"Recording saved at {file_path}")
                ti.xcom_push(key=f"recording_status_{call_id}", value="Recording Saved")
                logger.info(f"Pushed XCom: key=recording_status_{call_id}, value=Recording Saved")
                return {"message": "Recording downloaded successfully", "file_path": file_path}
            else:
                logger.error(f"Failed to download recording, status code: {response.status_code}")
                ti.xcom_push(key=f"recording_status_{call_id}", value="Recording Failed")
                logger.info(f"Pushed XCom: key=recording_status_{call_id}, value=Recording Failed")
                return {"message": "Failed to download recording"}
        except Exception as e:
            logger.error(f"Failed to fetch/save recording: {str(e)}")
            ti.xcom_push(key=f"recording_status_{call_id}", value="Failed")
            return {"message": f"Failed to fetch/save recording: {str(e)}"}

    # Define tasks
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

    fetch_recording_task = PythonOperator(
        task_id="fetch_and_save_recording",
        python_callable=fetch_and_save_recording,
        provide_context=True
    )

    # Set task dependencies
    initiate_call_task >> wait_task >> check_status_task
    check_status_task >> fetch_recording_task