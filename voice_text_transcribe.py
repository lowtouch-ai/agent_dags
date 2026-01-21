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
import librosa  # For audio preprocessing
import soundfile as sf
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
    schedule=None,
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

    def preprocess_audio(audio_file_path):
        audio, sr = librosa.load(audio_file_path, sr=16000)
        audio = librosa.util.normalize(audio)
        temp_processed_file = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
        sf.write(temp_processed_file.name, audio, sr, format='wav')
        return temp_processed_file.name
    
    
    
    
    def transcribe_audio(**kwargs):
        """Decrypt and transcribe encrypted audio data with enhanced Whisper settings."""
        ti = kwargs["ti"]
        conf = kwargs["dag_run"].conf
        encrypted_audio = conf.get("encrypted_audio")
        call_id = conf.get("call_id")

        if not encrypted_audio:
            raise ValueError("'encrypted_audio' must be provided in the configuration.")

        # Decrypt the audio
        temp_file_path = None
        preprocessed_file_path = None
        try:
            fernet_key = Variable.get("FERNET_SECRET_KEY").encode()  # Retrieve key from Airflow Variables
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

            # Preprocess the audio
            logger.info("Preprocessing audio...")
            preprocessed_file_path = preprocess_audio(audio_file_path)
            logger.info(f"Preprocessed audio saved to: {preprocessed_file_path}")

            # Load a larger Whisper model for better accuracy
            logger.info("Loading Whisper model...")
            WHISPER_MODEL = whisper.load_model("medium")  # Upgrade to 'medium' for better accent handling
            logger.info("Whisper model loaded successfully")

            # Transcription with enhanced settings
            logger.info("Starting transcription...")
            start_time = datetime.now()
            result = WHISPER_MODEL.transcribe(
                preprocessed_file_path,
                language="en",           # Enforce English
                task="transcribe",
                beam_size=5,            # Enable beam search for better accuracy
                temperature=0.0,        # Low temperature to reduce hallucinations
                condition_on_previous_text=False  # Avoid context bias
            )
            transcription = result["text"].strip()
            logger.info(f"Whisper transcription: {transcription}")
            logger.info(f"Transcription time: {(datetime.now() - start_time).total_seconds():.2f} seconds")

            # Clean up temporary files
            for temp_path in [temp_file_path, preprocessed_file_path]:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
                    logger.info(f"Deleted temporary file {temp_path}")

            # Save transcription to Variable if call_id is provided
            if call_id:
                variable_key = f"text_{call_id}"
                Variable.set(variable_key, transcription)
                logger.info(f"Saved transcription to Variable {variable_key}")

            ti.xcom_push(key="transcription", value=transcription)
            return transcription

        except Exception as e:
            logger.error(f"Decryption or transcription failed: {str(e)}")
            # Clean up temporary files on error
            for temp_path in [temp_file_path, preprocessed_file_path]:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
                    logger.info(f"Deleted temporary file {temp_path} due to error")
            raise

    # Single task for transcribing
    transcribe_task = PythonOperator(
        task_id="transcribe_audio",
        python_callable=transcribe_audio,
        provide_context=True,
    )