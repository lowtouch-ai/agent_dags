from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import csv
from PyPDF2 import PdfReader
from ollama import Client

# new Shared Drive and folder settings
SHARED_DRIVE_ID = '0AO6Pw6zAUDLJUk9PVA'  # ✅ Actual Shared Drive ID
FOLDER_ID = '1sqk2IONrPJHtNruCMzAyYqOd3igXJmND'  # ✅ Folder inside the Shared Drive
CSV_FILENAME = 'cv_results.csv'
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
    description='Process one JD and multiple CVs via AgentOmatic and save result to Drive CSV',
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

    messages = [
        {"role": "user", "content": f"Job Description:\n{jd_text}"},
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
        existing_files = service.files().list(
            q=f"name='{filename}' and trashed=false",
            driveId=SHARED_DRIVE_ID,
            corpora="drive",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="files(id, name)"
        ).execute().get('files', [])

        for file in existing_files:
            try:
                service.files().delete(fileId=file['id'], supportsAllDrives=True).execute()
                print(f"✅ Deleted existing file: {file['name']}")
            except Exception as e:
                print(f"⚠️ Failed to delete existing file {file['name']}: {e}")
    except Exception as e:
        print(f"⚠️ Error checking existing files: {e}")

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

    results = []
    for cv in cvs:
        print(f"⚙️ Processing CV: {cv['name']}")
        agent_result = call_agent(jd_text, cv['text'], cv['name'])
        results.append({
            "cv_file": cv['name'],
            "score_or_result": agent_result
        })

    # Prepare CSV content
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["cv_file", "score_or_result"])
    writer.writeheader()
    for row in results:
        writer.writerow(row)

    upload_to_drive(
        service=service,
        content_bytes=csv_buffer.getvalue().encode('utf-8'),
        filename=CSV_FILENAME,
        folder_id=FOLDER_ID,
        mimetype='text/csv'
    )
    print(f"✅ Uploaded updated CSV with {len(results)} results to Drive.")

with dag:
    process_and_score_task = PythonOperator(
        task_id='process_and_score_cv_against_jd',
        python_callable=process_and_score
    )
