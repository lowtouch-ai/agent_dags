"""
Email utility functions for Gmail integration.
Handles authentication, fetching emails, and processing attachments.
"""

import os
import json
import time
import logging
import base64
import re
from email.utils import parseaddr
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def authenticate_gmail(credentials_json, expected_email):
    """
    Authenticate Gmail API and verify correct email account.
    
    Args:
        credentials_json: JSON string with Gmail OAuth credentials
        expected_email: Email address that should be authenticated
        
    Returns:
        Gmail service object or None if authentication fails
    """
    try:
        if not credentials_json:
            logging.error("Gmail credentials not provided")
            return None
        
        creds = Credentials.from_authorized_user_info(json.loads(credentials_json))
        service = build("gmail", "v1", credentials=creds)
        
        # Verify correct account
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        
        if logged_in_email.lower() != expected_email.lower():
            logging.error(f"Wrong Gmail account! Expected {expected_email}, got {logged_in_email}")
            return None
        
        logging.info(f"Successfully authenticated Gmail: {logged_in_email}")
        return service
        
    except Exception as e:
        logging.error(f"Gmail authentication error: {str(e)}")
        return None
   
    
def send_email(service, recipient, subject, body, in_reply_to, references, 
               from_address, cc=None, bcc=None, thread_id=None, agent_name="Recruitment Agent"):
    """
    Send email reply maintaining proper thread continuity with CC and BCC support.
    Uses multipart/alternative for better HTML rendering compatibility.
    
    Args:
        service: Gmail API service instance
        recipient: Email address to send to
        subject: Email subject (should start with "Re:" for replies)
        body: HTML body content
        in_reply_to: Message-ID of the email being replied to
        references: References header from original email
        from_address: Email address to send from
        cc: Comma-separated string or list of CC recipients (optional)
        bcc: Comma-separated string or list of BCC recipients (optional)
        thread_id: Gmail thread ID to maintain conversation
    """
    try:
        logging.debug(f"Preparing email to {recipient} with subject: {subject}")
        
        # Create multipart/alternative message for text/plain + text/html
        msg = MIMEMultipart('alternative')
        msg["From"] = f"{agent_name} via lowtouch.ai <{from_address}>"
        msg["To"] = recipient
        
        # Add CC recipients if provided
        if cc:
            if isinstance(cc, list):
                cc = ', '.join(cc)
            msg["Cc"] = cc
            logging.debug(f"Adding Cc: {cc}")
        
        # Add BCC recipients if provided
        if bcc:
            if isinstance(bcc, list):
                bcc = ', '.join(bcc)
            msg["Bcc"] = bcc
            logging.debug(f"Adding Bcc: {bcc}")
        
        # Ensure subject has "Re:" prefix for replies
        if not subject.lower().startswith("re:"):
            msg["Subject"] = f"Re: {subject}"
        else:
            msg["Subject"] = subject
        
        # CRITICAL: Set threading headers
        if in_reply_to:
            msg["In-Reply-To"] = in_reply_to
            logging.debug(f"Set In-Reply-To: {in_reply_to}")
        
        # Build References header: original references + message being replied to
        if references and in_reply_to:
            # Ensure in_reply_to isn't already in references to avoid duplication
            if in_reply_to not in references:
                msg["References"] = f"{references} {in_reply_to}".strip()
            else:
                msg["References"] = references
        elif in_reply_to:
            msg["References"] = in_reply_to
        elif references:
            msg["References"] = references
        
        if msg.get("References"):
            logging.debug(f"Set References: {msg['References']}")
        
        # Generate plain-text fallback by stripping HTML tags
        plain_body = re.sub(r'<[^>]+>', '', body)  # Strips all tags
        plain_body = re.sub(r'\s+', ' ', plain_body).strip()  # Normalize whitespace
        plain_body = re.sub(r'&nbsp;', ' ', plain_body)  # Replace HTML entities
        plain_body = re.sub(r'&[a-z]+;', '', plain_body)  # Remove other HTML entities
        
        # # Attach plain-text part first (fallback for non-HTML clients)
        # part_plain = MIMEText(plain_body, "plain", "utf-8")
        # msg.attach(part_plain)
        
        # Attach HTML part second (preferred by HTML-capable clients)
        part_html = MIMEText(body, "html", "utf-8")
        msg.attach(part_html)
        
        # Encode message for Gmail API
        raw_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        
        # Build request body with threadId if available
        request_body = {"raw": raw_msg}
        if thread_id:
            request_body["threadId"] = thread_id
            logging.debug(f"Sending to thread: {thread_id}")
        
        # Send email via Gmail API
        result = service.users().messages().send(
            userId="me",
            body=request_body
        ).execute()
        
        logging.info(f"Email sent successfully: {result.get('id')} in thread {result.get('threadId')}")
        logging.info(f"Recipients - To: {recipient}, Cc: {cc if cc else 'None'}, Bcc: {bcc if bcc else 'None'}")
        return result
        
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}", exc_info=True)
        return None


def mark_email_as_read(service, email_id):
    """Mark the email as read."""
    try:
        service.users().messages().modify(
            userId="me",
            id=email_id,
            body={"removeLabelIds": ["UNREAD"]}
        ).execute()
        logging.info(f"Marked email {email_id} as read.")
    except Exception as e:
        logging.error(f"Failed to mark email {email_id} as read: {str(e)}")


def extract_all_recipients(email_data):
    """
    Extract all recipients (To, Cc, Bcc) from email data.
    
    Args:
        email_data: Dictionary containing email headers
        
    Returns:
        dict: {'to': [], 'cc': [], 'bcc': []}
    """
    headers = email_data.get("headers", {})
    to_header = headers.get("To", "")
    cc_header = headers.get("Cc", "")
    bcc_header = headers.get("Bcc", "")
    
    def parse_recipients(header):
        recipients = []
        if header:
            # Simple parse; for production, use more robust parsing
            addrs = re.findall(r'<?([^> ,]+@[^> ,]+)>?', header)
            recipients = [addr for addr in addrs if '@' in addr]
        return recipients
    
    return {
        'to': parse_recipients(to_header),
        'cc': parse_recipients(cc_header),
        'bcc': parse_recipients(bcc_header)
    }

def get_last_checked_timestamp(file_path):
    """
    Retrieve the last processed email timestamp from file.
    
    Args:
        file_path: Path to JSON file storing timestamp
        
    Returns:
        Timestamp in milliseconds or current time if no previous timestamp
    """
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                last_checked = data.get("last_processed", None)
                if last_checked:
                    logging.info(f"Retrieved last processed timestamp: {last_checked}")
                    return last_checked
        except Exception as e:
            logging.error(f"Error reading timestamp file: {str(e)}")
    
    # Initialize with current timestamp
    current_timestamp_ms = int(time.time() * 1000)
    logging.info(f"No previous timestamp found, initializing to {current_timestamp_ms}")
    update_last_checked_timestamp(file_path, current_timestamp_ms)
    return current_timestamp_ms


def update_last_checked_timestamp(file_path, timestamp):
    """
    Store the last processed email timestamp to file.
    
    Args:
        file_path: Path to JSON file for storing timestamp
        timestamp: Timestamp in milliseconds to store
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            json.dump({"last_processed": timestamp}, f)
        logging.info(f"Updated last processed timestamp to {timestamp}")
    except Exception as e:
        logging.error(f"Error updating timestamp: {str(e)}")


def fetch_unread_emails_with_attachments(service, last_checked_timestamp, attachment_dir):
    """
    Fetch unread emails with attachments from Gmail.
    
    Args:
        service: Gmail API service object
        last_checked_timestamp: Timestamp in milliseconds of last check
        attachment_dir: Directory to save attachments
        
    Returns:
        List of email objects with extracted attachments
    """
    try:
        # Ensure attachment directory exists
        os.makedirs(attachment_dir, exist_ok=True)
        
        # Build search query
        query = f"is:unread after:{last_checked_timestamp // 1000} has:attachment"
        logging.info(f"Fetching emails with query: {query}")
        
        # Search for emails
        results = service.users().messages().list(
            userId="me",
            labelIds=["INBOX"],
            q=query
        ).execute()
        
        messages = results.get("messages", [])
        logging.info(f"Found {len(messages)} unread emails with attachments")
        
        unread_emails = []
        
        for msg in messages:
            try:
                # Get full message data
                msg_data = service.users().messages().get(
                    userId="me",
                    id=msg["id"],
                    format="full"
                ).execute()
                
                # Extract headers
                headers = {
                    header["name"]: header["value"]
                    for header in msg_data["payload"]["headers"]
                }
                
                sender = headers.get("From", "").lower()
                timestamp = int(msg_data["internalDate"])
                
                # Skip no-reply emails and old emails
                if "no-reply" in sender or timestamp <= last_checked_timestamp:
                    logging.info(f"Skipping email from {sender} (timestamp: {timestamp})")
                    continue
                
                # Extract email body
                body = msg_data.get("snippet", "")
                
                # Process attachments
                attachments = process_email_attachments(
                    service=service,
                    message_id=msg["id"],
                    payload=msg_data["payload"],
                    attachment_dir=attachment_dir
                )
                
                # Create email object
                email_object = {
                    "id": msg["id"],
                    "threadId": msg_data.get("threadId"),
                    "headers": headers,
                    "content": body,
                    "timestamp": timestamp,
                    "attachments": attachments
                }
                
                logging.info(
                    f"Processed email {msg['id']} from {headers.get('From', 'Unknown')} "
                    f"with {len(attachments)} attachment(s)"
                )
                
                unread_emails.append(email_object)
                
            except Exception as e:
                logging.error(f"Error processing message {msg['id']}: {str(e)}")
                continue
        
        return unread_emails
        
    except Exception as e:
        logging.error(f"Error fetching emails: {str(e)}")
        return []


def process_email_attachments(service, message_id, payload, attachment_dir):
    """
    Process and download email attachments.
    
    Args:
        service: Gmail API service object
        message_id: Email message ID
        payload: Email payload containing parts
        attachment_dir: Directory to save attachments
        
    Returns:
        List of attachment objects with metadata
    """
    from agent_dags.utils.agent_utils import (
        extract_pdf_content,
        convert_image_to_base64
    )
    
    attachments = []
    
    if "parts" not in payload:
        return attachments
    
    for part in payload.get("parts", []):
        try:
            if not part.get("filename") or not part.get("body", {}).get("attachmentId"):
                continue
            
            filename = part["filename"]
            mime_type = part.get("mimeType", "application/octet-stream")
            attachment_id = part["body"]["attachmentId"]
            
            logging.info(f"Processing attachment: {filename} (type: {mime_type})")
            
            # Download attachment
            attachment_data = service.users().messages().attachments().get(
                userId="me",
                messageId=message_id,
                id=attachment_id
            ).execute()
            
            file_data = base64.urlsafe_b64decode(attachment_data["data"].encode("UTF-8"))
            
            # Save attachment
            attachment_path = os.path.join(attachment_dir, f"{message_id}_{filename}")
            with open(attachment_path, "wb") as f:
                f.write(file_data)
            
            logging.info(f"Saved attachment: {filename} at {attachment_path}")
            
            # Process based on file type
            extracted_content = {}
            base64_content = ""
            
            if mime_type == "application/pdf":
                extracted_content = extract_pdf_content(attachment_path)
                logging.info(f"Extracted PDF content from {filename}")
            
            elif mime_type in ["image/png", "image/jpeg", "image/jpg"]:
                base64_content = convert_image_to_base64(attachment_path)
                logging.info(f"Converted image {filename} to base64")
            
            # Create attachment object
            attachments.append({
                "filename": filename,
                "mime_type": mime_type,
                "path": attachment_path,
                "extracted_content": extracted_content,
                "base64_content": base64_content
            })
            
        except Exception as e:
            logging.error(f"Error processing attachment {part.get('filename', 'unknown')}: {str(e)}")
            continue
    
    return attachments


