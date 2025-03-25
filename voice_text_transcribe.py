from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import whisper
import logging
from airflow.models import Variable
from cryptography.fernet import Fernet
from airflow.models import Variable
import os
import tempfile
import base64
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'voice_transcriber.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()
    
with DAG(
    "shared_transcribe_message_voice",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["shared", "message", "voice", "transcribe"],
    params={
        "file_path": {
            "type": ["object", "null"],
            "properties": {
                "value": {"type": "string", "description": "Path to the unencrypted MP3 file"}
            },
            "required": ["value"],
            "default": None
        },
        "encrypted_audio": {
            "type": ["string", "null"],
            "description": "Base64-encoded encrypted audio data",
            "default": None
        },
        "call_id": {"type": ["string", "null"], "description": "Optional call ID for tracking"}
    }
) as dag:

    def transcribe_audio(**kwargs):
        """Decrypt and transcribe encrypted audio data."""
        ti = kwargs["ti"]
        conf = kwargs["dag_run"].conf
        encrypted_audio = conf.get("encrypted_audio")
        call_id = conf.get("call_id")

        if not encrypted_audio:
            raise ValueError("'encrypted_audio' must be provided in the configuration.")

        # Decrypt the audio
        temp_file_path = None
        try:
            fernet_key = Variable.get("FERNET_KEY").encode()  # Retrieve key from Airflow Variables
            fernet = Fernet(fernet_key)
            encrypted_data = base64.b64decode(encrypted_audio.encode('utf-8'))
            decrypted_data = fernet.decrypt(encrypted_data)

            # Write decrypted data to a temporary file
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as temp_file:
                temp_file_path = temp_file.name
                temp_file.write(decrypted_data)
                temp_file.flush()
            audio_file_path = temp_file_path
            logger.info(f"Decrypted audio to temporary file: {audio_file_path}")

            # Load the model and transcribe
            logger.info("Loading Whisper model...")
            WHISPER_MODEL = whisper.load_model("small")
            logger.info("Whisper model loaded successfully")

            logger.info("Starting transcription...")
            start_time = datetime.now()
            result = WHISPER_MODEL.transcribe(
                audio_file_path,
                language="en",
                task="transcribe",
            )
            transcription = result["text"]
            logger.info(f"Whisper transcription: {transcription}")
            logger.info(f"Transcription time: {(datetime.now() - start_time).total_seconds():.2f} seconds")

            # Clean up the temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                logger.info(f"Deleted temporary decrypted file {temp_file_path}")

            # Save transcription to Variable if call_id is provided
            if call_id:
                variable_key = f"text_{call_id}"
                Variable.set(variable_key, transcription)
                logger.info(f"Saved transcription to Variable {variable_key}")

            ti.xcom_push(key="transcription", value=transcription)
            return transcription
        except Exception as e:
            logger.error(f"Decryption or transcription failed: {str(e)}")
            # Clean up temporary file if it exists
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                logger.info(f"Deleted temporary file {temp_file_path} due to error")
            raise

    # Single task for transcribing
    transcribe_task = PythonOperator(
        task_id="transcribe_audio",
        python_callable=transcribe_audio,
        provide_context=True,
    )