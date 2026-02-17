"""
Google Sheets utility functions for Airflow DAGs.
Provides authentication and data management for Google Sheets.
"""

import logging
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
from datetime import datetime

def authenticate_google_sheets_oauth(credentials_json):
    try:
        # Parse credentials
        if isinstance(credentials_json, str):
            credentials_dict = json.loads(credentials_json)
        else:
            credentials_dict = credentials_json

        # IMPORTANT: always reuse stored scopes
        scopes = credentials_dict.get(
            "scopes",
            ["https://www.googleapis.com/auth/spreadsheets"]
        )

        # Build credentials correctly
        credentials = Credentials(
            token=credentials_dict.get("access_token"),
            refresh_token=credentials_dict.get("refresh_token"),
            token_uri=credentials_dict.get("token_uri"),
            client_id=credentials_dict.get("client_id"),
            client_secret=credentials_dict.get("client_secret"),
            scopes=scopes,
        )

        # 🔑 CRITICAL FIX: refresh expired token
        if credentials.expired and credentials.refresh_token:
            logging.info("Refreshing expired Google OAuth token")
            credentials.refresh(Request())

        if not credentials.valid:
            raise Exception("Invalid Google OAuth credentials")

        service = build("sheets", "v4", credentials=credentials)
        logging.info("Authenticated with Google Sheets using OAuth")

        return service

    except Exception as e:
        logging.error(f"Google Sheets OAuth auth failed: {e}")
        return None


def authenticate_google_sheets_service_account(credentials_json):
    """
    Authenticate with Google Sheets API using service account credentials.
    
    Args:
        credentials_json (str): JSON string containing service account credentials
        
    Returns:
        service: Google Sheets API service object or None if authentication fails
    """
    try:
        # Parse credentials JSON
        if isinstance(credentials_json, str):
            credentials_dict = json.loads(credentials_json)
        else:
            credentials_dict = credentials_json
            
        # Define the required scopes
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        
        # Create credentials from service account info
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=SCOPES
        )
        
        # Build the Sheets API service
        service = build('sheets', 'v4', credentials=credentials)
        logging.info("Successfully authenticated with Google Sheets API using Service Account")
        
        return service
        
    except Exception as e:
        logging.error(f"Failed to authenticate with Google Sheets using Service Account: {str(e)}")
        return None


def authenticate_google_sheets(credentials_json, auth_type='oauth'):
    """
    Authenticate with Google Sheets API.
    Supports both OAuth2 and Service Account authentication.
    
    Args:
        credentials_json (str): JSON string containing credentials
        auth_type (str): Type of authentication - 'oauth' or 'service_account'
        
    Returns:
        service: Google Sheets API service object or None if authentication fails
    """
    if auth_type == 'oauth':
        return authenticate_google_sheets_oauth(credentials_json)
    elif auth_type == 'service_account':
        return authenticate_google_sheets_service_account(credentials_json)
    else:
        logging.error(f"Invalid auth_type: {auth_type}. Use 'oauth' or 'service_account'")
        return None


def get_available_sheets(service, spreadsheet_id):
    """
    Get list of all sheet names in a spreadsheet.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id (str): The ID of the spreadsheet
        
    Returns:
        list: List of sheet names
    """
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
        logging.info(f"Available sheets: {sheet_names}")
        return sheet_names
    except HttpError as error:
        logging.error(f"Error getting sheet list: {error}")
        return []


def validate_sheet_name(service, spreadsheet_id, sheet_name):
    """
    Validate if a sheet name exists in the spreadsheet.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id (str): The ID of the spreadsheet
        sheet_name (str): Name of the sheet to validate
        
    Returns:
        str: Valid sheet name or None if not found
    """
    try:
        available_sheets = get_available_sheets(service, spreadsheet_id)
        
        # Exact match
        if sheet_name in available_sheets:
            return sheet_name
        
        # Case-insensitive match
        for available in available_sheets:
            if available.lower() == sheet_name.lower():
                logging.warning(f"Sheet name mismatch: using '{available}' instead of '{sheet_name}'")
                return available
        
        # If no match found, log available sheets and return first one as fallback
        logging.error(f"Sheet '{sheet_name}' not found. Available sheets: {available_sheets}")
        if available_sheets:
            fallback = available_sheets[0]
            logging.warning(f"Using fallback sheet: '{fallback}'")
            return fallback
        
        return None
        
    except Exception as e:
        logging.error(f"Error validating sheet name: {e}")
        return None


def get_sheet_headers(service, spreadsheet_id, sheet_name='Sheet1'):
    """
    Get the header row from a Google Sheet.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id (str): The ID of the spreadsheet
        sheet_name (str): Name of the sheet (default: 'Sheet1')
        
    Returns:
        list: List of header values or None if failed
    """
    try:
        # Validate sheet name first
        valid_sheet = validate_sheet_name(service, spreadsheet_id, sheet_name)
        if not valid_sheet:
            logging.error(f"Cannot get headers: sheet '{sheet_name}' does not exist")
            return None
        
        range_name = f"'{valid_sheet}'!A1:Z1"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        headers = result.get('values', [[]])[0]
        logging.info(f"Retrieved {len(headers)} headers from sheet '{valid_sheet}'")
        return headers
        
    except HttpError as error:
        logging.error(f"Error retrieving sheet headers: {error}")
        return None


def append_candidate_to_sheet(service, spreadsheet_id, candidate_data, sheet_name='Sheet1'):
    """
    Append candidate data to a Google Sheet.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id (str): The ID of the spreadsheet
        candidate_data (dict): Dictionary containing candidate information
        sheet_name (str): Name of the sheet (default: 'Sheet1')
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Validate sheet name first
        valid_sheet = validate_sheet_name(service, spreadsheet_id, sheet_name)
        if not valid_sheet:
            logging.error(f"Cannot append data: sheet '{sheet_name}' does not exist")
            return False
        
        # Get existing headers to ensure proper column mapping
        headers = get_sheet_headers(service, spreadsheet_id, valid_sheet)
        
        # If no headers exist, create them
        if not headers:
            headers = [
                'Timestamp',
                'Candidate Email',
                'Candidate Name',
                'Job Title',
                'Total Score',
                'Must Have Score',
                'Nice to Have Score',
                'Other Criteria Score',
                'Eligible',
                'Remarks',
                'Experience (Years)',
                'Education',
                'Email Subject',
                'Sender',
                'Status'
            ]
            
            # Write headers
            header_range = f"'{valid_sheet}'!A1:O1"
            header_body = {'values': [headers]}
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=header_range,
                valueInputOption='RAW',
                body=header_body
            ).execute()
            logging.info(f"Created headers in Google Sheet '{valid_sheet}'")
        
        # Prepare row data based on headers
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        row_data = [
            timestamp,
            candidate_data.get('candidate_email', ''),
            candidate_data.get('candidate_name', ''),
            candidate_data.get('job_title', ''),
            candidate_data.get('total_score', 0),
            candidate_data.get('must_have_score', 0),
            candidate_data.get('nice_to_have_score', 0),
            candidate_data.get('other_criteria_score', 0),
            'Yes' if candidate_data.get('eligible', False) else 'No',
            candidate_data.get('remarks', ''),
            candidate_data.get('experience_years', ''),
            candidate_data.get('education', ''),
            candidate_data.get('email_subject', ''),
            candidate_data.get('sender', ''),
            candidate_data.get('status', 'Processed')
        ]
        
        # Append the row
        range_name = f"'{valid_sheet}'!A:O"
        body = {'values': [row_data]}
        
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        logging.info(f"Successfully appended candidate data to '{valid_sheet}'. Updated cells: {result.get('updates').get('updatedCells')}")
        return True
        
    except HttpError as error:
        logging.error(f"Error appending to Google Sheet: {error}")
        return False


def update_candidate_status(service, spreadsheet_id, candidate_email, new_status, sheet_name='Sheet1'):
    """
    Update the status of a candidate in the Google Sheet.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id (str): The ID of the spreadsheet
        candidate_email (str): Email of the candidate to update
        new_status (str): New status value
        sheet_name (str): Name of the sheet (default: 'Sheet1')
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Validate sheet name first
        valid_sheet = validate_sheet_name(service, spreadsheet_id, sheet_name)
        if not valid_sheet:
            logging.error(f"Cannot update status: sheet '{sheet_name}' does not exist")
            return False
        
        # Get all data from the sheet
        range_name = f"'{valid_sheet}'!A:O"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logging.warning(f"No data found in sheet '{valid_sheet}'")
            return False
        
        # Find the candidate's row (assuming email is in column B)
        email_column_index = 1  # Column B (0-indexed)
        status_column_index = 14  # Column O (0-indexed)
        
        for i, row in enumerate(values[1:], start=2):  # Skip header row
            if len(row) > email_column_index and row[email_column_index] == candidate_email:
                # Update the status
                update_range = f"'{valid_sheet}'!O{i}"
                body = {'values': [[new_status]]}
                
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=update_range,
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                logging.info(f"Updated status for {candidate_email} to {new_status} in '{valid_sheet}'")
                return True
        
        logging.warning(f"Candidate {candidate_email} not found in sheet '{valid_sheet}'")
        return False
        
    except HttpError as error:
        logging.error(f"Error updating candidate status: {error}")
        return False


def find_candidate_in_sheet(service, spreadsheet_id, candidate_email, sheet_name='Sheet1'):
    """
    Check if a candidate already exists in the Google Sheet.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id (str): The ID of the spreadsheet
        candidate_email (str): Email of the candidate to find
        sheet_name (str): Name of the sheet (default: 'Sheet1')
        
    Returns:
        dict: Candidate data if found, None otherwise
    """
    try:
        # Validate sheet name first
        valid_sheet = validate_sheet_name(service, spreadsheet_id, sheet_name)
        if not valid_sheet:
            logging.error(f"Cannot find candidate: sheet '{sheet_name}' does not exist")
            return None
        
        range_name = f"'{valid_sheet}'!A:O"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values or len(values) < 2:
            return None
        
        headers = values[0]
        email_column_index = 1  # Column B
        
        for row in values[1:]:
            if len(row) > email_column_index and row[email_column_index] == candidate_email:
                # Return candidate data as dictionary
                return dict(zip(headers, row))
        
        return None
        
    except HttpError as error:
        logging.error(f"Error finding candidate in sheet: {error}")
        return None


def get_candidate_details(service, spreadsheet_id, candidate_email, sheet_name='Candidates'):
    """
    Retrieve detailed candidate information from Google Sheets.
    
    Args:
        service: Google Sheets API service instance
        spreadsheet_id: ID of the Google Sheet
        candidate_email: Email address of the candidate to find
        sheet_name: Name of the sheet to search in (default: 'Candidates')
    
    Returns:
        dict: Candidate profile with all available fields, or None if not found
    """
    try:
        # Validate sheet name first
        valid_sheet = validate_sheet_name(service, spreadsheet_id, sheet_name)
        if not valid_sheet:
            logging.error(f"Cannot get candidate details: sheet '{sheet_name}' does not exist")
            return None
        
        # First, find the candidate row
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{valid_sheet}'!A:Z"  # Assuming data doesn't extend beyond column Z
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logging.warning(f"No data found in sheet: {valid_sheet}")
            return None
        
        # Get header row (first row)
        headers = values[0]
        
        # Find email column index
        try:
            email_col_index = headers.index('Candidate Email')
        except ValueError:
            logging.error("Could not find 'Candidate Email' column in sheet")
            return None
        
        # Search for the candidate
        for row_index, row in enumerate(values[1:], start=2):  # Skip header
            if len(row) > email_col_index and row[email_col_index] == candidate_email:
                # Found the candidate, build profile dictionary
                candidate_profile = {}
                
                for col_index, header in enumerate(headers):
                    # Get value if it exists in this row
                    value = row[col_index] if col_index < len(row) else ''
                    
                    # Convert header to snake_case key
                    key = header.lower().replace(' ', '_').replace('/', '_')
                    
                    # Special handling for certain fields
                    if key in ['total_score', 'must_have_score', 'nice_to_have_score', 'other_criteria_score']:
                        # Convert scores to integers
                        try:
                            candidate_profile[key] = int(value) if value else 0
                        except ValueError:
                            candidate_profile[key] = 0
                    elif key == 'eligible':
                        # Convert to boolean
                        candidate_profile[key] = value.lower() in ['true', 'yes', '1'] if value else False
                    else:
                        candidate_profile[key] = value
                
                logging.info(f"Retrieved profile for candidate: {candidate_email} from '{valid_sheet}'")
                return candidate_profile
        
        # Candidate not found
        logging.info(f"Candidate not found in sheet '{valid_sheet}': {candidate_email}")
        return None
        
    except Exception as e:
        logging.error(f"Error retrieving candidate details: {str(e)}")
        return None


def get_candidate_status(service, spreadsheet_id, candidate_email, sheet_name='Candidates'):
    """
    Get just the status of a candidate from Google Sheets.
    
    Args:
        service: Google Sheets API service instance
        spreadsheet_id: ID of the Google Sheet
        candidate_email: Email address of the candidate
        sheet_name: Name of the sheet to search in (default: 'Candidates')
    
    Returns:
        str: Candidate status or None if not found
    """
    try:
        candidate = get_candidate_details(service, spreadsheet_id, candidate_email, sheet_name)
        return candidate.get('status') if candidate else None
    except Exception as e:
        logging.error(f"Error getting candidate status: {str(e)}")
        return None


def check_candidate_screening_sent(service, spreadsheet_id, candidate_email, sheet_name='Candidates'):
    """
    Check if screening questions have been sent to a candidate.
    
    Args:
        service: Google Sheets API service instance
        spreadsheet_id: ID of the Google Sheet
        candidate_email: Email address of the candidate
        sheet_name: Name of the sheet to search in (default: 'Candidates')
    
    Returns:
        bool: True if screening was sent (status is 'Email Sent'), False otherwise
    """
    try:
        status = get_candidate_status(service, spreadsheet_id, candidate_email, sheet_name)
        return status == 'Email Sent' if status else False
    except Exception as e:
        logging.error(f"Error checking screening status: {str(e)}")
        return False


def get_all_candidates(service, spreadsheet_id, sheet_name='Candidates'):
    """
    Retrieve all candidates from Google Sheets.
    
    Args:
        service: Google Sheets API service instance
        spreadsheet_id: ID of the Google Sheet
        sheet_name: Name of the sheet to read from (default: 'Candidates')
    
    Returns:
        list: List of candidate dictionaries
    """
    try:
        # Validate sheet name first
        valid_sheet = validate_sheet_name(service, spreadsheet_id, sheet_name)
        if not valid_sheet:
            logging.error(f"Cannot get candidates: sheet '{sheet_name}' does not exist")
            return []
        
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{valid_sheet}'!A:Z"
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logging.warning(f"No data found in sheet: {valid_sheet}")
            return []
        
        # Get header row
        headers = values[0]
        candidates = []
        
        # Process each row
        for row in values[1:]:  # Skip header
            candidate = {}
            for col_index, header in enumerate(headers):
                key = header.lower().replace(' ', '_').replace('/', '_')
                value = row[col_index] if col_index < len(row) else ''
                
                # Type conversion
                if key in ['total_score', 'must_have_score', 'nice_to_have_score', 'other_criteria_score']:
                    try:
                        candidate[key] = int(value) if value else 0
                    except ValueError:
                        candidate[key] = 0
                elif key == 'eligible':
                    candidate[key] = value.lower() in ['true', 'yes', '1'] if value else False
                else:
                    candidate[key] = value
            
            candidates.append(candidate)
        
        logging.info(f"Retrieved {len(candidates)} candidates from sheet '{valid_sheet}'")
        return candidates
        
    except Exception as e:
        logging.error(f"Error retrieving all candidates: {str(e)}")
        return []