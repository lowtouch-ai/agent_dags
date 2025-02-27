from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from twilio.rest import Client
import json
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Default args for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG with Params
with DAG(
    "send-voice-message",
    default_args=default_args,
    schedule_interval=None,  # Triggered manually
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "message": Param("Hello, this is a test call.", type="string", minLength=5, maxLength=500, title="Voice Message", description="Enter the message to be spoken in the call."),
        "phone_number": Param("+1234567890", type="string", title="Phone Number", description="Enter the recipient's phone number in E.164 format."),
        "need_ack": Param(False, type="boolean", title="Require Acknowledgment", description="Set to true if you need an acknowledgment.")
    }
) as dag:

    class TwilioVoiceCall:
        """Class to handle automated voice calls using Twilio API."""
        def __init__(self):
            self.account_sid = Variable.get("TWILIO_ACCOUNT_SID")
            self.auth_token = Variable.get("TWILIO_AUTH_TOKEN")
            self.twilio_phone = Variable.get("TWILIO_PHONE_NUMBER")
            if not all([self.account_sid, self.auth_token, self.twilio_phone]):
                raise ValueError("Twilio credentials are missing. Check environment variables.")
            self.client = Client(self.account_sid, self.auth_token)

        def initiate_call(self, message: str, phone_number: str) -> str:
            """Initiate a voice call and return the call SID."""
            if not message or not phone_number:
                raise ValueError("Both 'message' and 'phone_number' are required.")
            call = self.client.calls.create(
                to=phone_number,
                from_=self.twilio_phone,
                twiml=f'<Response><Say>{message}</Say></Response>',
                status_callback_event=["completed"]
            )
            logger.info(f"Call initiated with SID: {call.sid}")
            return call.sid

        def check_call_status(self, call_sid: str) -> str:
            """Check the status of a call by its SID."""
            call = self.client.calls(call_sid).fetch()
            logger.info(f"Call status: {call.status}")
            if call.status in ["completed", "no-answer"] and call.duration:
                return f"Call completed with status: {call.status} and duration: {call.duration} seconds."
            return f"Current call status: {call.status}"

    def send_voice_message(**kwargs):
        """Task to initiate the voice call."""
        conf = kwargs['params']
        logger.info(f"Received params: {json.dumps(conf, indent=2)}")
        message = conf["message"]
        phone_number = conf["phone_number"]
        need_ack = conf.get("need_ack", False)

        if not message or not phone_number:
            raise ValueError("Missing required parameters: 'message' and 'phone_number'")

        twilio_call = TwilioVoiceCall()
        call_sid = twilio_call.initiate_call(message, phone_number)
        kwargs['ti'].xcom_push(key='call_sid', value=call_sid)
        return {"call_sid": call_sid, "need_ack": need_ack}

    def check_call_status(**kwargs):
        """Task to check the call status."""
        ti = kwargs['ti']
        call_sid = ti.xcom_pull(key='call_sid', task_ids='send_voice_message')
        need_ack = ti.xcom_pull(task_ids='send_voice_message')['need_ack']

        if not need_ack:
            return "Acknowledgment not required, skipping status check."

        twilio_call = TwilioVoiceCall()
        status = twilio_call.check_call_status(call_sid)

        if "completed" in status or "no-answer" in status:
            return status
        raise ValueError(f"Call not yet completed: {status}")

    # Define tasks
    send_call_task = PythonOperator(
        task_id="send_voice_message",
        python_callable=send_voice_message,
        provide_context=True
    )

    wait_task = TimeDeltaSensor(
        task_id="wait_for_call_status",
        delta=timedelta(seconds=5),  # Wait 5 seconds between checks
        poke_interval=5,  # Check every 5 seconds (adjustable)
    )

    check_status_task = PythonOperator(
        task_id="check_call_status",
        python_callable=check_call_status,
        provide_context=True,
        retries=9,  # Poll up to 10 times (total ~50 seconds)
        retry_delay=timedelta(seconds=5)
    )

    # Set task dependencies
    send_call_task >> wait_task >> check_status_task