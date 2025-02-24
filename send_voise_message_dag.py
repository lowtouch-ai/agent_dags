from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from twilio_test import TwilioVoiceCall  # Import the Twilio class

# Default args for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def send_voice_message(**kwargs):
    """Send a voice message using TwilioVoiceCall class"""
    conf = kwargs['dag_run'].conf
    message = conf.get("message")
    phone_number = conf.get("phone_number")
    need_ack = conf.get("need_ack", False)

    if not message or not phone_number:
        raise ValueError("Missing required parameters: 'message' and 'phone_number'")

    # Instantiate the TwilioVoiceCall class
    twilio_call = TwilioVoiceCall()

    # Call the method to send a voice message
    response = twilio_call.send_call(message, phone_number, need_ack)

    return response

# Define DAG
with DAG("send-voice-message",
         default_args=default_args,
         schedule_interval=None,  # Triggered manually
         catchup=False) as dag:

    send_call_task = PythonOperator(
        task_id="send_voice_message",
        python_callable=send_voice_message,
        provide_context=True
    )

    send_call_task
