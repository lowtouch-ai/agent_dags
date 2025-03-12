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

def load_whisper_model(**kwargs):
    """Load the Whisper model and push it to XCom."""
    ti = kwargs["ti"]
    try:
        logger.info("Loading Whisper model...")
        WHISPER_MODEL = whisper.load_model("small")
        logger.info("Whisper model loaded successfully")
        # Push the model to XCom
        ti.xcom_push(key="whisper_model", value=WHISPER_MODEL)
        return True
    except Exception as e:
        logger.error(f"Failed to load Whisper model: {str(e)}")
        raise

def transcribe_with_whisper(file_path, **kwargs):
    """Transcribe audio using the Whisper model pulled from XCom."""
    ti = kwargs["ti"]
    start_time = time.time()
    
    try:
        # Pull the loaded model from XCom
        WHISPER_MODEL = ti.xcom_pull(key="whisper_model", task_ids="load_whisper_model")
        if not WHISPER_MODEL:
            raise ValueError("Whisper model not found in XCom. Ensure the load_whisper_model task ran successfully.")
        
        logger.info("Starting transcription...")
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
        "file_path": {
            "type": "object",
            "properties": {
                "value": {"type": "string", "description": "Path to the MP3 file to transcribe"}
            },
            "required": ["value"]
        },
        "call_id": {"type": ["string", "null"], "description": "Optional call ID for tracking"}
    }
) as dag:

    # Task 1: Load the Whisper model
    load_model_task = PythonOperator(
        task_id="load_whisper_model",
        python_callable=load_whisper_model,
        provide_context=True,
    )

    # Task 2: Transcribe the audio using the loaded model
    transcribe_task = PythonOperator(
        task_id="transcribe_audio",
        python_callable=transcribe_with_whisper,
        op_kwargs={"file_path": "{{ params.file_path.value }}"},
        provide_context=True,
    )

    # Set task dependencies: transcribe_task depends on load_model_task
    load_model_task >> transcribe_task