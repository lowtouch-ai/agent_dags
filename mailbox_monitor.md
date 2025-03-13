# webshop_mailbox_monitor

## Overview
- **Purpose**: Monitors the Gmail inbox specified by `WEBSHOP_FROM_ADDRESS` for unread emails received after the last processed timestamp. Triggers the `shared_send_message_email` DAG for each qualifying email to handle responses.
- **Maintained by**: 
  - Krishna Ullas (kullas@ecloudcontrol.com)

## Configuration
### Airflow Variables
- `WEBSHOP_FROM_ADDRESS`: The Gmail address to monitor
- `WEBSHOP_GMAIL_CREDENTIALS`: JSON-formatted Gmail API credentials

### DAG Parameters
- **Owner**: lowtouch.ai_developers
- **Schedule Interval**: Every 1 minute

### Workflow Components
1. **Fetch Unread Emails Task** (`fetch_unread_emails`):
   - Authenticates with Gmail API using provided credentials.
   - Fetches unread emails from the inbox after the last processed timestamp.
   - Filters out emails from "no-reply" senders or those before the last timestamp.
   - Stores email data (ID, thread ID, headers, content, timestamp) in XCom.
   - Updates the last processed timestamp if new emails are found.
2. **Trigger Response Task** (`trigger-email-response-dag`):
   - Retrieves unread emails from XCom.
   - Triggers the `shared_send_message_email` DAG for each unread email, passing email data in the `conf` parameter.
   - Clears the XCom `unread_emails` key after processing.
