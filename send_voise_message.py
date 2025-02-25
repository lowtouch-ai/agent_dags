from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from twilio.rest import Client
from airflow.models import Variable

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
    render_template_as_native_obj=True,  # Ensures correct type handling
    params={
        "message": Param("Hello, this is a test call.", type="string", minLength=5, maxLength=500, title="Voice Message", description="Enter the message to be spoken in the call."),
        "phone_number": Param("+1234567890", type="string", title="Phone Number", description="Enter the recipient's phone number in E.164 format."),
        "need_ack": Param(False, type="boolean", title="Require Acknowledgment", description="Set to true if you need an acknowledgment.")
    }
) as dag:

    def send_voice_message(**kwargs):
        """Send a voice message using TwilioVoiceCall class"""
        conf = kwargs['params']  # Accessing params

        # Log received parameters
        import json
        import logging
        logging.info(f"Received params: {json.dumps(conf, indent=2)}")

        message = conf["message"]
        phone_number = conf["phone_number"]
        need_ack = conf.get("need_ack", False)

        # Check required fields
        if not message or not phone_number:
            raise ValueError("Missing required parameters: 'message' and 'phone_number'")

        # Instantiate the TwilioVoiceCall class
        twilio_call = TwilioVoiceCall()

        # Call the method to send a voice message
        response = twilio_call.send_call(message, phone_number, need_ack)

        return response

    send_call_task = PythonOperator(
        task_id="send_voice_message",
        python_callable=send_voice_message
    )

    send_call_task


class TwilioVoiceCall:
    """
    Class to handle automated voice calls using Twilio API.
    """

    def __init__(self):
        """Initialize Twilio client using environment variables."""
        self.account_sid = Variable.get("TWILIO_ACCOUNT_SID")
        self.auth_token = Variable.get("TWILIO_AUTH_TOKEN")
        self.twilio_phone = Variable.get("TWILIO_PHONE_NUMBER")

        if not all([self.account_sid, self.auth_token, self.twilio_phone]):
            raise ValueError("Twilio credentials are missing. Check environment variables.")

        self.client = Client(self.account_sid, self.auth_token)

    def send_call(self, message: str, phone_number: str, need_ack: bool = False) -> str:
        """
        Send a voice call with a custom message.

        Args:
            message (str): The message to be spoken during the call.
            phone_number (str): Recipient's phone number.
            need_ack (bool): If True, waits for acknowledgment (simulated).

        Returns:
            str: Call status message.
        """
        if not message or not phone_number:
            raise ValueError("Both 'message' and 'phone_number' are required.")

        call = self.client.calls.create(
            to=phone_number,
            from_=self.twilio_phone,
            twiml=f'<Response><Say>{message}</Say></Response>',
            status_callback="https://yourserver.com/call_status",  # Webhook for status updates
            status_callback_event=["completed"]
        )

        if need_ack:
            return f"Call initiated. Call SID: {call.sid}. Awaiting acknowledgment..."
        return f"Call initiated successfully. Call SID: {call.sid}"
