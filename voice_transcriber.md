# autofinix_voice_transcriber

## Overview
- **Purpose**: Transcribes audio files (e.g., MP3 recordings from Twilio calls) using the Whisper "small" model and stores the transcription in an Airflow Variable for use by triggering DAGs.
- **Maintained by**: 
  - Ajay Raj (arajc@ecloudcontrol.com)

### DAG Parameters
- **Owner**: lowtouch.ai_developers
- **Schedule Interval**: None (triggered externally)
- **Params**:
  - `file_path`: Object with a `value` property (string) specifying the path to the MP3 file to transcribe (required).
  - `call_id`: Optional string or null, used to store transcription in a Variable (`text_{call_id}`) for cross-DAG access.

### Workflow Components
1. **Transcribe Audio** (`transcribe_audio`):
   - **Input**: Receives `file_path` from `params` and `call_id` from `dag_run.conf`.
   - **Process**:
     1. Loads the Whisper "small" model.
     2. Transcribes the audio file in English.
     3. Stores transcription in Variable `text_{call_id}` (if `call_id` is provided).
     4. Pushes transcription to XCom (`transcription` key).
   - **Output**: Returns the transcribed text or raises an exception on failure.
