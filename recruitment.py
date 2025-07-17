from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import pandas as pd
import json
from PyPDF2 import PdfReader
from ollama import Client

# 📁 Google Drive Shared Drive and Folder Config
SHARED_DRIVE_ID = '0AO6Pw6zAUDLJUk9PVA'
FOLDER_ID = '1sqk2IONrPJHtNruCMzAyYqOd3igXJmND'
MODEL_NAME = 'recruitment-agent:0.3'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cv_agent_processing',
    default_args=default_args,
    description='Score CVs using JD and Agent, save structured JSON as CSV to Drive',
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
    text = ''
    for page in reader.pages:
        text += page.extract_text() or ''
    return text.strip()

def call_agent(jd_text, cv_text, cv_file_name):
    agent_url = f"http://{Variable.get('AGENT_HOST', default_var='agentomatic:8000')}/"
    client = Client(host=agent_url)

    instructions = """
You are a CV evaluation agent. Given a job description and candidate CV, evaluate the CV and return the result in the following strict JSON format:

{
  "Name": "",
  "Email": "",
  "Phone Number": "",
  "Overall Score": "",
  "Must-Have Criteria": "",
  "Nice-to-Have Criteria": "",
  "Other Criteria": "",
  "Notes": "",
  "Remarks": ""
}

Respond only with valid JSON.
"""

    messages = [
        {"role": "user", "content": f"{instructions}\nJob Description:\n{jd_text}"},
        {"role": "user", "content": f"Candidate CV:\n{cv_text}"}
    ]

    response = client.chat(
        model=MODEL_NAME,
        messages=messages,
        stream=False
    )
    return response['message']['content']

def upload_to_drive(service, content_bytes, filename, folder_id, mimetype):
    media = MediaIoBaseUpload(io.BytesIO(content_bytes), mimetype=mimetype)
    try:
        service.files().create(
            body={'name': filename, 'parents': [folder_id]},
            media_body=media,
            supportsAllDrives=True,
            fields='id'
        ).execute()
        print(f"✅ Uploaded file: {filename}")
    except Exception as e:
        print(f"❌ Failed to upload file {filename}: {e}")
        raise

def process_and_score(ti, **kwargs):
    service = get_drive_service()
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        fields="files(id, name)"
    ).execute()

    files = results.get('files', [])
    jd_text = None
    cvs = []

    for file in files:
        file_id = file['id']
        file_name = file['name'].lower()
        pdf_bytes = service.files().get_media(fileId=file_id).execute()
        extracted_text = extract_text_from_pdf_bytes(pdf_bytes)

        if 'jd' in file_name or 'job description' in file_name:
            jd_text = extracted_text
            print(f"✅ Found JD: {file['name']}")
        else:
            cvs.append({
                'name': file['name'],
                'text': extracted_text
            })

    if not jd_text:
        raise ValueError("❗ No JD found in Drive folder.")

    structured_results = []
    for cv in cvs:
        print(f"⚙️ Processing CV: {cv['name']}")
        agent_output = call_agent(jd_text, cv['text'], cv['name'])

        try:
            data = json.loads(agent_output)
            structured_results.append(data)
        except json.JSONDecodeError:
            print(f"❌ JSON parsing failed for {cv['name']}")
            structured_results.append({
                "Name": cv['name'],
                "Email": "",
                "Phone Number": "",
                "Overall Score": "",
                "Must-Have Criteria": "",
                "Nice-to-Have Criteria": "",
                "Other Criteria": "",
                "Notes": "Failed to parse JSON",
                "Remarks": agent_output[:1000]  # Keep raw response snippet
            })

    df = pd.DataFrame(structured_results)

    timestamp_str = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    dynamic_filename = f"cv_results_{timestamp_str}.csv"

    csv_bytes = df.to_csv(index=False).encode('utf-8')
    upload_to_drive(
        service=service,
        content_bytes=csv_bytes,
        filename=dynamic_filename,
        folder_id=FOLDER_ID,
        mimetype='text/csv'
    )
    print(f"✅ Uploaded structured CSV '{dynamic_filename}' with {len(df)} entries to Drive.")

with dag:
    process_and_score_task = PythonOperator(
        task_id='process_and_score_cv_against_jd',
        python_callable=process_and_score
    )
