# autofinix_reminder_check

## Overview
- **Purpose**: Monitors the Autofinix API for loan reminders with status "Reminder," generates voice messages using AgentOmatic, triggers voice calls via the `shared_send_message_voice` DAG, and updates reminder statuses based on call outcomes and transcriptions.
- **Maintained by**: 
  - Ajay Raj (arajc@ecloudcontrol.com)

## Configuration
### Airflow Variables
- `AUTOFINIX_API_URL`: Base URL for the Autofinix API.
- `AGENTOMATIC_API_URL`: URL of the AgentOmatic server hosting the AI model.
- `AUTOFINIX_TEST_PHONE_NUMBER`: Test phone number for loan ID.
- `AUTOFINIX_DEMO_PHONE_ODD`: Demo phone number for odd-numbered loan IDs.
- `AUTOFINIX_DEMO_PHONE_EVEN`: Demo phone number for even-numbered loan IDs.

### DAG Parameters
- **Owner**: lowtouch.ai_developers
- **Schedule Interval**: Every 1 minute

### Workflow Components
1. **Fetch Due Loans** (`fetch_due_loans`):
   - Queries Autofinix API for reminders with `status=Reminder`.
   - Enriches reminders with customer data and assigns phone numbers (test/demo based on loan ID).
   - Pushes eligible loans to XCom (`eligible_loans`).
2. **Evaluate Due Loans Result** (`evaluate_due_loans_result`):
   - Branches to `generate_voice_message` if loans are found, otherwise to `handle_no_due_loans`.
3. **Generate Voice Message** (`generate_voice_message`):
   - Updates reminder status to `CallInitiated` via API.
   - Generates AI-driven voice messages using AgentOmatic (`autofinix:0.3`).
   - Pushes voice payloads and call IDs to XCom.
4. **Trigger Twilio Voice Call** (`trigger_twilio_voice_call`):
   - Triggers `shared_send_message_voice` DAG for each call ID with voice payload.
   - Waits for completion.
5. **Update Call Status** (`update_call_status`):
   - Retrieves Twilio status and transcription from Airflow Variables.
   - Updates Autofinix API with call status (`CallCompleted`, `CallNotAnswered`, `CallFailed`) and transcription (if available).
   - Analyzes transcription with AgentOmatic to set new reminder dates (if call completed).
   - Deletes temporary Variables and pushes outcomes to XCom.
6. **Update Reminder Status** (`update_reminder_status`):
   - Logs final call outcomes and transcriptions for reporting.
7. **Handle No Due Loans** & **End Task**:
   - Dummy tasks for workflow branching and completion.
