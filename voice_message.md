# autofinix_voice_messenger

## Overview
- **Purpose**: Initiates voice calls via Twilio, delivers a pre-defined message, optionally records responses, and triggers the `shared_transcribe_message_voice` DAG for transcription. Stores call outcomes, recordings, and transcriptions for use by triggering DAGs (e.g., `autofinix_check_reminders_due`).
- **Maintained by**: 
  - Ajay Raj (arajc@ecloudcontrol.com)

## Configuration
### Airflow Variables
- `TWILIO_ACCOUNT_SID`: Twilio account SID for API authentication.
- `TWILIO_AUTH_TOKEN`: Twilio auth token for API authentication.
- `TWILIO_PHONE_NUMBER`: Twilio phone number used as the caller ID.

### DAG Parameters
- **Owner**: lowtouch.ai_developers
- **Schedule Interval**: None (triggered externally)
- **Params**:
  - `phone_number`: Target phone number (string, e.g., "+1234567890").
  - `message`: Message to play before the beep (string).
  - `need_ack`: Whether to record a response (boolean, default: False).
  - `call_id`: Unique call identifier (string or null, defaults to UUID if null).

### Workflow Components
1. **Initiate Call** (`initiate_call`):
   - Starts a Twilio voice call with the provided `phone_number` and `message`.
   - If `need_ack=True`, records up to 30 seconds of response.
   - Pushes `call_sid`, `need_ack`, and `call_id` to XCom.
2. **Wait for Call Status** (`wait_for_call_status`):
   - Delays 30 seconds to allow call initiation.
3. **Check Call Status** (`check_call_status`):
   - Polls Twilio API for call status (retries up to 50 times).
   - Stores final status (`completed`, `no-answer`, `busy`, `failed`) in XCom and Variable (`twilio_call_status_{call_id}`).
4. **Branch Recording Logic** (`branch_recording_logic`):
   - If status is `completed`, proceeds to `fetch_and_save_recording`; otherwise, skips to `skip_recording`.
5. **Fetch and Save Recording** (`fetch_and_save_recording`):
   - Downloads recording (if `need_ack=True` and status=`completed`).
   - Saves to `/appz/data/send-voice-message/{execution_date}/{call_sid}.mp3`.
   - Stores file path in XCom and Variable (`twilio_recording_file_{call_id}`).
6. **Prepare Transcription Trigger** (`prepare_transcription_trigger`):
   - Prepares configuration for transcription DAG with recording file path and `call_id`.
7. **Trigger Transcription DAG** (`trigger_transcription_dag`):
   - Triggers `shared_transcribe_message_voice` with recording details.
8. **Wait for Transcription** (`wait_for_transcription`):
   - Delays 5 seconds between transcription fetch attempts.
9. **Fetch Transcription** (`fetch_transcription`):
   - Retrieves transcription from Variable (`text_{call_id}`), set by the transcription DAG.
   - Stores in Variable (`twilio_transcription_{call_id}`) and XCom.
10. **Skip Recording** (`skip_recording`):
    - Dummy task for non-recording path.
