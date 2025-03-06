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

TWILIO_ACCOUNT_SID = Variable.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = Variable.get("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = Variable.get("TWILIO_PHONE_NUMBER")

if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
    raise ValueError("Twilio credentials are missing. Set them in Airflow Variables.")

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
        response = session.get(url, auth=auth, timeout=10)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

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
    }
) as dag:

    # ... (initiate_call and check_call_status remain unchanged as they use Twilio client)

    def fetch_and_save_recording(base_storage_dir, **kwargs):
        """Fetch and save recording from Twilio"""
        ti = kwargs["ti"]
        conf = kwargs["params"]

        call_sid = ti.xcom_pull(task_ids="initiate_call", key="call_sid")
        final_status = ti.xcom_pull(task_ids="check_call_status", key="call_status_final")
        need_ack = ti.xcom_pull(task_ids="initiate_call", key="need_ack")
        call_id = conf.get("call_id", None)

        if not need_ack:
            logger.info("need_ack is False, skipping recording download.")
            ti.xcom_push(key="recording_status", value="No Recording Needed")
            return {"message": "No acknowledgment required."}

        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        recordings = client.recordings.list(call_sid=call_sid)

        if not recordings:
            logger.info("Recording not found yet. Try again later.")
            ti.xcom_push(key="recording_status", value="Recording Not Found")
            return {"message": "Recording not available yet. Try again later."}

        recording_url = f"https://api.twilio.com{recordings[0].uri.replace('.json', '.mp3')}"
        execution_date = datetime.now().strftime("%Y-%m-%d")
        dag_name = "send-voice-message-1"
        save_directory = os.path.join(base_storage_dir, dag_name, execution_date)
        os.makedirs(save_directory, exist_ok=True)
        file_path = os.path.join(save_directory, f"{call_sid}.mp3")

        try:
            response = make_api_request(
                recording_url,
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
            )
            with open(file_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Recording saved at {file_path}")
            ti.xcom_push(key="recording_status", value="Recording Saved")
            return {"message": "Recording downloaded successfully", "file_path": file_path}
        except Exception as e:
            logger.error(f"Failed to download recording: {str(e)}")
            ti.xcom_push(key="recording_status", value="Recording Failed")
            return {"message": "Failed to download recording"}

    # ... (rest of the DAG definition remains unchanged)