# webshop_email_responder

## Overview
- **Purpose**: Responds to unread emails detected by the `webshop_monitor_mailbox` DAG. Uses an AI model (via Ollama) to generate responses and sends them via Gmail, maintaining email threading.
- **Maintained by**: 
  - Krishna Ullas (kullas@ecloudcontrol.com)

## Configuration
### Airflow Variables
- `WEBSHOP_FROM_ADDRESS`: The Gmail address used to send responses.
- `WEBSHOP_GMAIL_CREDENTIALS`: JSON-formatted Gmail API credentials.
- `WEBSHOP_OLLAMA_HOST`: URL of the Ollama server hosting the AI model for response generation.

### DAG Parameters
- **Owner**: lowtouch.ai_developers
- **Schedule Interval**: None (triggered externally)

### Workflow Components
1. **Send Response Task** (`send-response`):
   - **Input**: Receives `email_data` via `dag_run.conf` from the triggering DAG (`webshop_monitor_mailbox`).
   - **Steps**:
     1. Authenticates with Gmail API using `WEBSHOP_GMAIL_CREDENTIALS`.
     2. Extracts sender, subject, and content from the email data.
     3. Generates an AI response using the Ollama model (`webshop-invoice:0.5`).
     4. Sends an HTML-formatted reply email, preserving threading with `In-Reply-To` and `References` headers.
   - **Fallback**: If errors occur (e.g., no content or AI failure), sends a default message: "We are currently experiencing technical difficulties. Please check back later."
