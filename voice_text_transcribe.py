from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import whisper
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def transcribe_with_whisper(file_path, **kwargs):
    """Transcribe audio using the Whisper model."""
    ti = kwargs["ti"]
    start_time = time.time()
    
    try:
        WHISPER_MODEL = whisper.load_model("small")
        logger.info("Whisper model loaded")
        result = WHISPER_MODEL.transcribe(
            file_path,
            language="en",
            task="transcribe",
        )
        transcription = result["text"]
        logger.info(f"Whisper transcription: {transcription}")
        logger.info(f"Transcription time: {time.time() - start_time:.2f} seconds")
        
        ti.xcom_push(key="transcription", value=transcription)
        return transcription
    except Exception as e:
        logger.error(f"Whisper transcription failed: {str(e)}")
        raise

with DAG(
    "voice_text_transcribe",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "file_path": {"type": "string", "description": "Path to the MP3 file to transcribe"},
        "call_id": {"type": ["string", "null"], "description": "Optional call ID for tracking"}
    }
) as dag:

    transcribe_task = PythonOperator(
        task_id="transcribe_audio",
        python_callable=transcribe_with_whisper,
        op_kwargs={"file_path": "{{ params.file_path }}"},
        provide_context=True,
    )

    transcribe_task