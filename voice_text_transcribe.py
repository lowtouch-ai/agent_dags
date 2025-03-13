from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import whisper
import logging
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def transcribe_audio(file_path, **kwargs):
    """Load the Whisper model and transcribe the audio in one task."""
    ti = kwargs["ti"]
    conf = kwargs["dag_run"].conf  # Access the conf from the triggered DAG
    call_id = conf.get("call_id")
    
    try:
        # Load the model and transcribe in one task
        logger.info("Loading Whisper model...")
        WHISPER_MODEL = whisper.load_model("small")
        logger.info("Whisper model loaded successfully")

        logger.info("Starting transcription...")
        start_time = datetime.now()
        result = WHISPER_MODEL.transcribe(
            file_path,
            language="en",
            task="transcribe",
        )
        transcription = result["text"]
        logger.info(f"Whisper transcription: {transcription}")
        logger.info(f"Transcription time: {(datetime.now() - start_time).total_seconds():.2f} seconds")

        # Save transcription to Variable only if call_id is provided
        if call_id:
            variable_key = f"text_{call_id}"
            Variable.set(variable_key, transcription)
            logger.info(f"Saved transcription to Variable {variable_key}")

        ti.xcom_push(key="transcription", value=transcription)
        return transcription
    except Exception as e:
        logger.error(f"Whisper transcription failed: {str(e)}")
        raise
        
readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'README.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()
    
with DAG(
    "shared_transcribe_message_voice",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["autofinix", "process", "voice"],
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

    # Single task for loading model and transcribing
    transcribe_task = PythonOperator(
        task_id="transcribe_audio",
        python_callable=transcribe_audio,
        op_kwargs={"file_path": "{{ params.file_path.value }}"},
        provide_context=True,
    )
