from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from ollama import Client

import io
from PyPDF2 import PdfReader
import pandas as pd
import os
import logging

# Constants
FOLDER_ID = '1cFl0s4IkZi-pPhZ4mRoAFm_gj9wEF8g1'  # Replace with your folder ID
CSV_FILENAME = 'agent_results.csv'
MODEL_NAME = 'recruitment-agent:0.3'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cv_process_one_by_one',
    default_args=default_args,
    description='Process each CV one-by-one with the same JD and append results to Drive CSV',
    schedule_interval=None,
    catchup=False
)

def get_drive_service():
    service_account_info = Variable.get("gdrive_credentials_json", deserialize_json=True)
    creds = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

def extract_text_from_pdf_bytes(pdf_bytes):
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = ''
    for page in reader.pages:
        text += page.extract_text() or ''
    return text.strip()

def call_recruitment_agent(jd_text, cv_text):
    client = Client()  # ✅ No model argument here
    response = client.chat(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": "You are a recruitment assistant."},
            {"role": "user", "content": f"JD:\n{jd_text}\n\nCV:\n{cv_text}"}
        ]
    )
    return response['message']['content']

def process_and_append():
    service = get_drive_service()

    # Get list of PDF files in the folder
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/pdf' and trashed = false",
        fields="files(id, name)",
    ).execute()
    files = results.get('files', [])

    if not files:
        logging.info("No PDF files found.")
        return

    jd_text = None
    cvs = []

    for file in files:
        file_id = file['id']
        file_name = file['name'].lower()
        pdf_bytes = service.files().get_media(fileId=file_id).execute()
        text = extract_text_from_pdf_bytes(pdf_bytes)

        if 'jd' in file_name or 'job description' in file_name:
            jd_text = text
            logging.info(f"✅ JD found: {file['name']}")
        else:
            cvs.append({
                'file_id': file_id,
                'file_name': file['name'],
                'text': text
            })

    if not jd_text:
        raise ValueError("No JD file found in folder.")

    # Try to download existing CSV
    csv_file_id = None
    existing_csv = None

    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and name='{CSV_FILENAME}' and mimeType='text/csv'",
        fields="files(id, name)"
    ).execute()
    items = results.get('files', [])
    if items:
        csv_file_id = items[0]['id']
        file_content = service.files().get_media(fileId=csv_file_id).execute()
        existing_csv = pd.read_csv(io.BytesIO(file_content))
    else:
        existing_csv = pd.DataFrame(columns=['cv_file', 'agent_response'])

    # Process each CV and append result
    for cv in cvs:
        logging.info(f"📄 Processing CV: {cv['file_name']}")
        agent_response = call_recruitment_agent(jd_text, cv['text'])
        new_row = pd.DataFrame([{
            'cv_file': cv['file_name'],
            'agent_response': agent_response
        }])
        existing_csv = pd.concat([existing_csv, new_row], ignore_index=True)

    # Save CSV to buffer
    csv_buffer = io.BytesIO()
    existing_csv.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv')

    if csv_file_id:
        # Update existing file
        service.files().update(fileId=csv_file_id, media_body=media).execute()
        logging.info(f"✅ Updated existing CSV: {CSV_FILENAME}")
    else:
        # Upload new file
        file_metadata = {
            'name': CSV_FILENAME,
            'parents': [FOLDER_ID],
            'mimeType': 'text/csv'
        }
        service.files().create(body=file_metadata, media_body=media).execute()
        logging.info(f"✅ Uploaded new CSV: {CSV_FILENAME}")

with dag:
    process_each_cv_and_append = PythonOperator(
        task_id='process_each_cv_and_append',
        python_callable=process_and_append
    )
