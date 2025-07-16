from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from ollama import Client
import io
from PyPDF2 import PdfReader
import csv
import os

# Constants
FOLDER_ID = '1cFl0s4IkZi-pPhZ4mRoAFm_gj9wEF8g1'  # Update if needed
CSV_FILENAME = "agent_results.csv"
MODEL_NAME = "recruitment-agent:0.3"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cv_process_one_by_one',
    default_args=default_args,
    description='One JD and each CV sent to recruitment agent and append result to CSV in Drive',
    schedule_interval=None,
    catchup=False,
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
    text = ''.join([page.extract_text() or '' for page in reader.pages])
    return text.strip()

def download_csv_file(service, folder_id, filename):
    """Download existing CSV if exists in Drive"""
    query = f"'{folder_id}' in parents and name='{filename}' and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    if files:
        file_id = files[0]['id']
        request = service.files().get_media(fileId=file_id)
        data = request.execute()
        return data.decode("utf-8"), file_id
    return None, None

def upload_csv_to_drive(service, folder_id, filename, content, file_id=None):
    media_body = io.BytesIO(content.encode("utf-8"))
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = {'mimeType': 'text/csv', 'body': media_body}

    if file_id:
        service.files().update(fileId=file_id, media_body=media['body']).execute()
    else:
        service.files().create(body=file_metadata, media_body=media['body'], fields='id').execute()

def call_recruitment_agent(jd_text, cv_text):
    client = Client(model=MODEL_NAME)
    response = client.chat(messages=[
        {"role": "system", "content": "You are a recruitment assistant."},
        {"role": "user", "content": f"JD:\n{jd_text}\n\nCV:\n{cv_text}"}
    ])
    return response['message']['content']

def process_and_append(**kwargs):
    service = get_drive_service()

    # Fetch PDF files in the folder
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/pdf' and trashed = false",
        fields="files(id, name)"
    ).execute()

    files = results.get('files', [])
    if not files:
        print("❗ No PDF files found.")
        return

    jd_text = None
    cv_files = []

    for file in files:
        name_lower = file['name'].lower()
        file_bytes = service.files().get_media(fileId=file['id']).execute()
        text = extract_text_from_pdf_bytes(file_bytes)

        if 'jd' in name_lower or 'job description' in name_lower:
            jd_text = text
            print(f"✅ JD found: {file['name']}")
        else:
            cv_files.append((file['name'], text))

    if not jd_text:
        print("❌ No JD file found.")
        return

    # Load existing CSV or start new one
    existing_csv, csv_file_id = download_csv_file(service, FOLDER_ID, CSV_FILENAME)
    rows = []
    if existing_csv:
        reader = csv.reader(io.StringIO(existing_csv))
        rows = list(reader)
    else:
        rows.append(["CV File", "Agent Response"])

    # Process each CV
    for cv_file, cv_text in cv_files:
        print(f"📄 Processing CV: {cv_file}")
        response = call_recruitment_agent(jd_text, cv_text)
        rows.append([cv_file, response.strip()[:1000]])  # Limiting long responses if needed

    # Write back CSV to buffer
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerows(rows)
    upload_csv_to_drive(service, FOLDER_ID, CSV_FILENAME, csv_buffer.getvalue(), csv_file_id)
    print("✅ CSV updated in Drive.")

with dag:
    process_task = PythonOperator(
        task_id='process_each_cv_and_append',
        python_callable=process_and_append,
        provide_context=True,
    )
